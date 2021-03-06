#
# Check if CXX compiler can compile ObjectiveC sources.
# The code was borrowed from CMakeTestCXXCompiler.cmake
#

if(CMAKE_OBJCXX_COMPILER_FORCED OR CMAKE_CXX_COMPILER_FORCED)
  # The compiler configuration was forced by the user.
  # Assume the user has configured all compiler information.
  set(CMAKE_OBJCXX_COMPILER_WORKS TRUE)
  return()
endif()

if(NOT CMAKE_OBJCXX_COMPILER_WORKS)
  message(STATUS "Check for working ObjectiveC++ compiler: ${CMAKE_CXX_COMPILER}")
  file(WRITE ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeTmp/testOBJCXXCompiler.mm
    "#if !defined(__OBJC__) || !defined(__cplusplus)\n"
    "# error \"The CMAKE_CXX_COMPILER doesn't support ObjectiveC++\"\n"
    "#endif\n"
    "int main(){return 0;}\n")
  try_compile(CMAKE_OBJCXX_COMPILER_WORKS ${CMAKE_BINARY_DIR} 
    ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeTmp/testOBJCXXCompiler.mm
    OUTPUT_VARIABLE __CMAKE_OBJCXX_COMPILER_OUTPUT)
  # Move result from cache to normal variable.
  set(CMAKE_OBJCXX_COMPILER_WORKS ${CMAKE_OBJCXX_COMPILER_WORKS})
  set(OBJCXX_TEST_WAS_RUN 1)
endif()

if(NOT CMAKE_OBJCXX_COMPILER_WORKS)
  message(STATUS "Check for working ObjectiveC++ compiler: "
    "${CMAKE_CXX_COMPILER}" " -- broken")
  file(APPEND ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeError.log
    "Determining if the ObjectiveC++ compiler works failed with "
    "the following output:\n${__CMAKE_OBJCXX_COMPILER_OUTPUT}\n\n")
  message(FATAL_ERROR "The ObjectiveC++ compiler \"${CMAKE_CXX_COMPILER}\" "
    "is not able to compile a simple ObjectiveC++ test program.\nIt fails "
    "with the following output:\n ${__CMAKE_OBJCXX_COMPILER_OUTPUT}\n\n"
    "CMake will not be able to correctly generate this project.")
else()
  if(OBJCXX_TEST_WAS_RUN)
    message(STATUS "Check for working ObjectiveC++ compiler: "
        "${CMAKE_CXX_COMPILER}" " -- works")
    file(APPEND ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeOutput.log
      "Determining if the ObjectiveC++ compiler works passed with "
      "the following output:\n${__CMAKE_OBJCXX_COMPILER_OUTPUT}\n\n")
  endif()
endif()

unset(__CMAKE_OBJCXX_COMPILER_OUTPUT)
