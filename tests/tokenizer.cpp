#include <string>

#include "csql.h"
#include "sql/parser.h"

int main() {
  csql::SQLTokenizer tokenizer(
      "create table users ({key, autoincrement} id :int32, {unique} login: string[32] = "
      "\"hello abc\", password_hash: bytes[8] = 0x0011223344556677, is_admin: bool = false)");
  while (tokenizer.hasNext()) {
    csql::Token token = tokenizer.nextToken();
    std::string type = csql::tokenTypeToString(token.type);
    printf("Type: %s, Value: %s\n", type.c_str(), token.value.c_str());
  }
  return 0;
}