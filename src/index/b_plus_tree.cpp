#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) { // root_page_id
        Page* page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
        auto root_page = reinterpret_cast<IndexRootsPage*>(page->GetData());
        // root_page->GetRootId(index_id,&root_page_id_);
        if (!root_page->GetRootId(index_id,&root_page_id_)) {
          root_page_id_ = INVALID_PAGE_ID;
        }
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
        // buffer_pool_manager_->UnpinPage(root_page_id_, true); // test

        // 如果传入的leaf_max_size和internal_max_size是默认值0，即UNDEFINED_SIZE，那么需要自己根据keysize进行计算
        if (leaf_max_size_ == UNDEFINED_SIZE) {
          leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId));
        }
        if (internal_max_size_ == UNDEFINED_SIZE) {
          // internal_max_size_ = (PAGE_SIZE - sizeof(InternalPage)) / (processor_.GetKeySize() + sizeof(page_id_t));
          internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(page_id_t));
        }
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  if (IsEmpty()) {
    return;
  }
  // buffer_pool_manager_->DeletePage(current_page_id); // no direct delete!!!
  if(current_page_id == INVALID_PAGE_ID) {
    current_page_id = root_page_id_;
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(2);
  }
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData());
  if(!page->IsLeafPage()) { // recursively
    auto *inner = reinterpret_cast<InternalPage *>(page);
    for(int i = page->GetSize() - 1; i >= 0; --i) {
      Destroy(inner->ValueAt(i));
    }
  }
  buffer_pool_manager_->DeletePage(page->GetPageId());
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) { 
  if (IsEmpty()){
    return false;
  }
  auto* Page = FindLeafPage(key);
  if (Page == nullptr){
    return false;
  }
  auto* leaf = reinterpret_cast<LeafPage*>(Page->GetData());
  RowId value;
  if (leaf->Lookup(key, value, processor_)){
    result.push_back(value);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return false;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) { 
  if (IsEmpty()){
    // LOG(INFO) << "BPlusTree::Insert first key" << std::endl;
    StartNewTree(key, value);
    return true;
  } 
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  auto* page = buffer_pool_manager_->NewPage(root_page_id_); // new page
  auto* root = reinterpret_cast<LeafPage*>(page->GetData());

  root->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  root->Insert(key, value, processor_);
  UpdateRootPageId(1);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) { 
  auto* Page = FindLeafPage(key);
  auto* leaf_page = reinterpret_cast<LeafPage*>(Page->GetData());
  RowId val;
  // cout << 666 << endl;
  if (leaf_page->Lookup(key, val, processor_)){
    // cout << 777 << endl;
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  leaf_page->Insert(key, value, processor_); // insert
  if (leaf_page->GetSize() >= leaf_max_size_){ // split
    auto* new_page = Split(leaf_page, transaction);
    InsertIntoParent(leaf_page, new_page->KeyAt(0), new_page, transaction);
    // buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true; 
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) { 
  page_id_t new_page_id;
  auto* Page = buffer_pool_manager_->NewPage(new_page_id);
  if (Page == nullptr){
    return nullptr;
  }
  auto* new_page = reinterpret_cast<InternalPage*>(Page->GetData());
  new_page->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize());
  node->MoveHalfTo(new_page, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return new_page;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) { 
  page_id_t new_page_id;
  auto* Page = buffer_pool_manager_->NewPage(new_page_id);
  if (Page == nullptr){
    return nullptr;
  }
  auto* new_page = reinterpret_cast<LeafPage*>(Page->GetData());
  new_page->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize());
  new_page->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_page_id); // at leaf, need direct insert
  node->MoveHalfTo(new_page);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return new_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if (old_node->IsRootPage()){
    auto* page = buffer_pool_manager_->NewPage(root_page_id_);
    auto* root = reinterpret_cast<InternalPage*>(page->GetData());
    root->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  }
  else{
    auto *parent_page = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId())->GetData());
    int size = parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    if (size >= internal_max_size_) { // need father split
      auto *parent_sib = Split(parent_page, transaction); // father sibling
      InsertIntoParent(parent_page, parent_sib->KeyAt(0), parent_sib, transaction);
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if (IsEmpty()){
    return;
  }
  auto* page = FindLeafPage(key);
  auto* leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
  int now_siz = leaf_page->GetSize();
  if (now_siz > leaf_page->RemoveAndDeleteRecord(key, processor_)){
    CoalesceOrRedistribute(leaf_page, transaction); // need to redistribute or merge
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  }
  else{
    // LOG(ERROR) << "Remove() : RemoveAndDeleteRecord() failed";
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  }
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  if (node->IsRootPage()){
    return AdjustRoot(node); //
  }
  page_id_t parent_id = node->GetParentPageId();
  auto* parent = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  int now_index = parent->ValueIndex(node->GetPageId());

  if (node->GetSize() >= node->GetMinSize()){
    // buffer_pool_manager_->UnpinPage(parent_id, false);
    return false;
  }

  int sibling_index = (now_index == 0) ? 1 : now_index - 1;
  page_id_t sibling_id = parent->ValueAt(sibling_index);
  auto* sibling = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(sibling_id)->GetData()); // get sibling
  int flag = 0;
  if (node->GetSize()+sibling->GetSize() > node->GetMaxSize()){
    Redistribute(sibling, node, now_index);
    buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_id, true);
  }
  else{
    if (now_index == 0){
      Coalesce(node, sibling, parent, 1, transaction);
    }
    else{
      Coalesce(sibling, node, parent, now_index, transaction);
    }
    buffer_pool_manager_->UnpinPage(sibling->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    flag = 1;
  }
  return flag;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  node->MoveAllTo(neighbor_node);
  parent->Remove(index);
  if (parent->GetSize() < parent->GetMinSize()){ 
    return CoalesceOrRedistribute<BPlusTree::InternalPage>(parent, transaction);
  }
  return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
  parent->Remove(index);
  if (parent->GetSize() < parent->GetMinSize()){
    return CoalesceOrRedistribute<BPlusTree::InternalPage>(parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  auto* parent = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  if (index == 0){
    neighbor_node->MoveFirstToEndOf(node);
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  }
  else{
    neighbor_node->MoveLastToFrontOf(node);
    parent->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  auto* parent = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  if (index == 0){
    neighbor_node->MoveFirstToEndOf(node, parent->KeyAt(1), buffer_pool_manager_);
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  }
  else{
    neighbor_node->MoveLastToFrontOf(node, parent->KeyAt(index), buffer_pool_manager_);
    parent->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  // LOG(INFO) << "Adjust was  called";
  if (old_root_node->GetSize() > 1){
    return false;
  }
  if (!old_root_node->GetSize() && old_root_node->IsLeafPage()){
    // return true;
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }
  else if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1){
    auto root = reinterpret_cast<InternalPage*>(old_root_node);
    auto* child = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(root->ValueAt(0))->GetData());
    child->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = child->GetPageId(); // set as new root
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  auto* page = reinterpret_cast<LeafPage*>(FindLeafPage(nullptr, INVALID_PAGE_ID, true)->GetData());
  page_id_t page_id = page->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return IndexIterator(page_id, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
   auto* page = reinterpret_cast<LeafPage*>(FindLeafPage(key, INVALID_PAGE_ID, false)->GetData());
    page_id_t page_id = page->GetPageId();
    int index = page->KeyIndex(key, processor_); // find index of the key
    buffer_pool_manager_->UnpinPage(page_id, false);
    return IndexIterator(page_id, buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() { // ??
// LOG(INFO)
  return IndexIterator(); 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) { // the use of page_id?
  // if(page_id == INVALID_PAGE_ID) page_id = root_page_id_;

  Page *page = buffer_pool_manager_->FetchPage(root_page_id_); // start from root page
  auto* current_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
  page_id_t now_page_id = root_page_id_;
  while(!current_page->IsLeafPage()){
    buffer_pool_manager_->UnpinPage(now_page_id, false);
    auto* internal_page = reinterpret_cast<InternalPage*>(current_page);
    if (leftMost == true){
      now_page_id = internal_page->ValueAt(0); // go left
    }
    else{
      now_page_id = internal_page->Lookup(key, processor_);
    }
    page = buffer_pool_manager_->FetchPage(now_page_id);
    current_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
  }
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto* rootpage = reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if (insert_record == 1){
    rootpage->Insert(index_id_, root_page_id_); // insert instead of update
  } else if (insert_record == 0){
    rootpage->Update(index_id_, root_page_id_);
  }
  else{
    rootpage->Delete(index_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  cout << "size: " << page->GetSize() << endl;
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}