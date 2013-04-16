project(tnt_snap)

set (util_snap "tarantool_snap")
set (util_snap_sources sha1.c main.c config.c space.c options.c indexate.c ref.c cursor.c snapshot.c update.m
                       ${CMAKE_SOURCE_DIR}/third_party/crc32.c)
set (util_snap_libs tntrpl tntnet tnt)

list(APPEND util_snap_sources
    ${common_sources}
    ${CMAKE_SOURCE_DIR}/cfg/tarantool_box_cfg.c
    ${CMAKE_SOURCE_DIR}/cfg/prscfg.c
)

list(APPEND util_snap_libs gopt coro misc ev eio)

set_source_files_properties(${CMAKE_SOURCE_DIR}/cfg/tarantool_box_cfg.c
    ${CMAKE_SOURCE_DIR}/cfg/prscfg.c PROPERTIES COMPILE_FLAGS
    "-Wno-unused-parameter"
    GENERATED True)

set_source_files_properties(${util_snap_sources} PROPERTIES OBJECT_DEPENDS
		            ${CMAKE_SOURCE_DIR}/cfg/tarantool_box_cfg.h)

set_source_files_compile_flags(${util_snap_sources})
add_executable(${util_snap} ${util_snap_sources})

target_link_libraries (${util_snap} ${util_snap_libs})

install (TARGETS ${util_snap} DESTINATION bin)
