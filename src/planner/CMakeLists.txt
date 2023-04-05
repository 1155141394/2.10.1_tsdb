# include Python.h 环境
include_directories("/usr/include/python3.10")
# 添加动态库
LINK_DIRECTORIES("/usr/lib/python3.10/config-3.10-x86_64-linux-gnu")

set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/planner.c
    ${CMAKE_CURRENT_SOURCE_DIR}/add_hashagg.c
    ${CMAKE_CURRENT_SOURCE_DIR}/agg_bookend.c
    ${CMAKE_CURRENT_SOURCE_DIR}/constify_now.c
    ${CMAKE_CURRENT_SOURCE_DIR}/constraint_cleanup.c
    ${CMAKE_CURRENT_SOURCE_DIR}/expand_hypertable.c
    ${CMAKE_CURRENT_SOURCE_DIR}/partialize.c
    ${CMAKE_CURRENT_SOURCE_DIR}/space_constraint.c
    ${CMAKE_CURRENT_SOURCE_DIR}/get_parameter.c    )
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})

target_link_libraries(${PROJECT_NAME} -lpython3.10)
