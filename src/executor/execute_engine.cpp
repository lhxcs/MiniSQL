#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

extern "C" {
extern int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}
ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_)
    current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  assert (ast->type_ == kNodeCreateTable);
  if (ast->child_ == nullptr){
    cout << "No table name." << endl;
    return DB_FAILED;
  }
  if (current_db_.empty()) {
    cout << "No database used." << endl;
    return DB_FAILED;
  }
  DBStorageEngine* db = dbs_[current_db_];
  string table_name = ast->child_->val_;
  TableInfo* table_info = nullptr;
  if (db->catalog_mgr_->GetTable(table_name, table_info) == DB_SUCCESS) {
    cout << "Table already exists." << endl;
    return DB_TABLE_ALREADY_EXIST;
  }
  vector<string> col_names;
  vector<string> col_types;
  vector<string> unique_keys;
  vector<string> primary_keys;
  map<string, bool> unique;
  map<string, bool> primary;
  map<string, int> char_size;
  pSyntaxNode col = ast->child_->next_->child_; 
  while (col->type_ == kNodeColumnDefinition && col != nullptr){ // begin to visit all column definitions
    // cout << 111 << endl;
    string col_name = col->child_->val_;
    string col_type = col->child_->next_->val_;
    // cout << 222 << endl;
    bool is_unique = false;
    if (col->val_ != nullptr) is_unique = !strcmp(col->val_, "unique"); // strcmp can't use null as parameter!!!!!!!!
    // cout << 33 << endl;
    if (col_type == "char"){ // check whether the size is invalid
      string col_len = col->child_->next_->child_->val_;
      if (col_len[0] == '-'){
        cout << "negative size." << endl;
        return DB_FAILED;
      }
      for (int i = 0; i < col_len.length(); ++i){
        char ch = col_len[i];
        if (ch == '.'){
          cout << "not integer size." << endl;
          return DB_FAILED;
        }
      }
      char_size[col_name] = stoi(col_len);
    }
    // cout << 44 << endl;
    col_names.push_back(col_name);
    col_types.push_back(col_type);
    if (is_unique){
      unique_keys.push_back(col_name);
      unique[col_name] = true;
    }
    col = col->next_;
  }
  if (col != nullptr && col->val_ != nullptr && !strcmp(col->val_, "primary keys")){ // have primary key
    // cout << 222 << endl;
    auto primary_nodes = col->child_;
    if (primary_nodes != nullptr){
      string primary_key_name = primary_nodes->val_;
      primary[primary_key_name] = true;
      primary_keys.push_back(primary_key_name);
      primary_nodes = primary_nodes->next_;
    }

    if (primary_nodes != nullptr){ // multiple primary keys
      cout << "multiple primary keys." << endl;
      return DB_FAILED;
    }
  }
  vector<Column*> columns;
  for (int i = 0; i < col_names.size(); ++i){ // create columns;
    // cout << 333 << endl;
    Column* col;
    string col_name = col_names[i];
    string col_type = col_types[i];
    bool flag = unique[col_name] || primary[col_name];
    if (col_type == "int"){
      col = new Column(col_name, TypeId::kTypeInt, i, true, flag);
    }
    else if (col_type == "char"){
      col = new Column(col_name, TypeId::kTypeChar, char_size[col_name], i, true, flag);
    }
    else if (col_type == "float"){
      col = new Column(col_name, TypeId::kTypeFloat, i, true, flag);
    }
    else{
      cout << "invalid type???" << endl;
      return DB_FAILED;
    }
    columns.push_back(col);
  }
  Schema* schema = new Schema(columns);
  // cout << 444 << endl;
  dberr_t createtable_result = db->catalog_mgr_->CreateTable(table_name, schema, nullptr, table_info);
  if (createtable_result != DB_SUCCESS){
    cout << "Table create failed." << endl;
    return createtable_result;
  }
  // cout << 555 << endl;
  IndexInfo* index_info;
  dberr_t createPindex_result = db->catalog_mgr_->CreateIndex(table_name, table_name +"_primary", primary_keys, nullptr, index_info, "bptree");
  if (createPindex_result != DB_SUCCESS){
    cout << "Primary index create failed." << endl;
    return createPindex_result;
  }
  for (int i = 0; i < unique_keys.size(); ++i){
    // cout << 666 << endl;
    dberr_t createUindex_result = db->catalog_mgr_->CreateIndex(table_name, table_name +"_unique_" + unique_keys[i], {unique_keys[i]}, nullptr, index_info, "bptree");
    if (createUindex_result != DB_SUCCESS){
      cout << "Unique index create failed." << endl;
      return createUindex_result;
    }
  }
  cout << "Table created." << endl;
  return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  assert(ast->type_ == kNodeDropTable);
  if (ast->child_ == nullptr){
    cout << "No table name." << endl;
    return DB_FAILED;
  }
  if (current_db_.empty()){
    cout << "No database used." << endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  // return dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
  // return DB_FAILED;
  dberr_t droptable_result = dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
  if (droptable_result != DB_SUCCESS){
    cout << "Table drop failed." << endl;
    return droptable_result;
  }
  cout << "Table dropped" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  assert(ast->type_ == kNodeShowIndexes);
  if (current_db_.empty()){
    cout << "No database used." << endl;
    return DB_FAILED;
  }
  vector<TableInfo*> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables); 
  for (auto table: tables){
    vector<IndexInfo*> indexes;
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table->GetTableName(), indexes);
    for (auto index: indexes){
      cout << "Table: " << table->GetTableName() << " Index: " << index->GetIndexName() << endl;
    }
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (current_db_.empty()){
    cout << "No database used." << endl;
    return DB_FAILED;
  }
  string index_name = ast->child_->val_;
  string table_name = ast->child_->next_->val_;
  pSyntaxNode key_node = ast->child_->next_->next_->child_;
  vector<string> keys;
  while (key_node != nullptr){
    string key_name = key_node->val_;
    keys.push_back(key_name);
    key_node = key_node->next_;
  }
  IndexInfo* index_info;
  dberr_t createindex_result = dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, keys, nullptr, index_info, "bptree");
  if (createindex_result != DB_SUCCESS){
    cout << "Index create failed." << endl;
    return createindex_result;
  }
  cout << "Index created." << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (current_db_.empty()){
    cout << "No database used." << endl;
    return DB_FAILED;
  }
  string index_name = ast->child_->val_;
  vector<TableInfo*> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables); // get all tables
  for (auto table: tables){
    if (dbs_[current_db_]->catalog_mgr_->DropIndex(table->GetTableName(), index_name) == DB_SUCCESS){
      cout << "Index dropped." << endl;
      return DB_SUCCESS;
    }
  }
  cout << "Index not found." << endl;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  char* file_name = ast->child_->val_;
  FILE* file = fopen(file_name, "r");
  if (file == nullptr){
    cout << "File not found." << endl;
    return DB_FAILED;
  }
  char ch[1024];
  clock_t start = clock();
  while(1){
    memset(ch, 0, sizeof(ch));
    int len = 0;
    char chr;
    while((chr = fgetc(file)) != ';'){ // read a sql command
      if (chr == EOF){
        clock_t end = clock();
        cout << "Execfile finished. Time: " << (double)(end - start) / CLOCKS_PER_SEC << "s" << endl;
        break;
      }
      ch[len++] = chr;
    }
    if (chr == EOF){
      break;
    }
    ch[len] = chr;
    YY_BUFFER_STATE bp = yy_scan_string(ch);
    yy_switch_to_buffer(bp);
    MinisqlParserInit();
    yyparse();
    if (MinisqlParserGetError()){
      printf("%s\n", "Error in parsing.");
      printf("%s\n", MinisqlParserGetErrorMessage());
    }
    auto execute_result = Execute(MinisqlGetParserRootNode()); // execute

    // clear
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();
    ExecuteInformation(execute_result);
    if (execute_result == DB_QUIT){
      return DB_QUIT;
    }
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
 assert(ast->type_ == kNodeQuit);
 return DB_QUIT;
}




// do not need implement
dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}
