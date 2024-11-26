file (GLOB_RECURSE SOURCES "src/*.cpp")
add_library(csql SHARED ${SOURCES})
target_include_directories(csql PUBLIC src)