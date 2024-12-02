#include <iostream>
#include <string>

#include "csql.h"

int main() {
  csql::Database db;

  db.execute(R"(
create table users (
  {key, autoincrement} id: int32,
  {unique} login: string[32],
  password_hash: bytes[8],
  is_admin: bool = false
);
create table posts (
  {key, autoincrement} id: int32,
  user_id: int32,
  title: string[32],
  content: string[1024] = ""
);
insert (login = "admin", password_hash = "12345678", is_admin = true) to users;
  )");
  for (int i = 0; i < 5; i++) {
    std::string login = "user" + std::to_string(i);
    std::string query = "insert (login = \"" + login + "\", password_hash = \"12345678\") to users";
    db.execute(query);

    for (int j = 0; j < 5; j++) {
      std::string title = login + "_title" + std::to_string(j);
      std::string content = login + "_content" + std::to_string(j);
      query = "insert (user_id = " + std::to_string(i + 1) + ", title = \"" + title +
              "\", content = \"" + content + "\") to posts";
      db.execute(query);
    }
  }

  db.execute(
      R"(
CREATE TABLE joined_posts AS (
  SELECT
    users.login as username,
    posts.title as title,
    posts.content as content 
  FROM (users JOIN posts ON users.id = posts.user_id) WHERE users.is_admin = false
);
)");

  db.exportTableToCSV("users", "users.csv");
  db.exportTableToCSV("posts", "posts.csv");
  db.exportTableToCSV("joined_posts", "joined_posts.csv");

  return 0;
}