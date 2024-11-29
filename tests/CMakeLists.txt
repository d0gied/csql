add_executable(main main.cpp)
target_link_libraries(main csql)

add_executable(tokenizer tokenizer.cpp)
target_link_libraries(tokenizer csql)


