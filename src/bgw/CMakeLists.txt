set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/job.c ${CMAKE_CURRENT_SOURCE_DIR}/job_stat.c
    ${CMAKE_CURRENT_SOURCE_DIR}/launcher_interface.c
    ${CMAKE_CURRENT_SOURCE_DIR}/scheduler.c ${CMAKE_CURRENT_SOURCE_DIR}/timer.c
    ${CMAKE_CURRENT_SOURCE_DIR}/s3supply.c    )
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
