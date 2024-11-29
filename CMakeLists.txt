cmake_minimum_required(VERSION 3.28)
project(CSQL)

set(CMAKE_CXX_STANDARD 20)

FIND_PACKAGE( Boost 1.40 COMPONENTS program_options REQUIRED )
INCLUDE_DIRECTORIES( ${Boost_INCLUDE_DIR} )


add_subdirectory(csql)
add_subdirectory(tests)
