#include <Python.h>
#include <stdio.h>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

#include "s3supply.h"

void *
_s3_supply_init(void* arg)
{
    int result = system("python3 /var/lib/postgresql/2.10.1_tsdb/src/PythonFile/map_matrix.py");
    if (result === -1){
        fprintf(stderr, "map_matrix failed!\n");
    } else{
        fprintf(stderr, "map_matrix start!\n");
    }

//    PyRun_SimpleString("import sys");
//    PyRun_SimpleString("sys.path.append('/var/lib/postgresql/2.10.1_tsdb/src/PythonFile')");
//    PyObject *pmodule = PyImport_ImportModule("map_matrix");
//    if (!pmodule)
//    {
//        fprintf(stderr, "cannot find map_matrix.py\n");
////        return -1;
//    }
//    else
//    {
//        fprintf(stderr, "PyImport_ImportModule success\n");
//    }
//
//    PyObject *pfunc = PyObject_GetAttrString(pmodule, "transfer_to_s3");
//    if (!pfunc)
//    {
//        fprintf(stderr, "cannot find func\n");
//        Py_XDECREF(pmodule);
////        return -1;
//    }
//    else
//    {
//        fprintf(stderr, "PyObject_GetAttrString success\n");
//    }
//    PyObject *pArgs = PyTuple_New(0);
//    PyObject *pResult = PyObject_CallObject(pfunc, pArgs);
////    fprintf(stderr, )
////    FILE *fp = fopen("~/test.txt", "w");
////    fprintf(fp, "Hello world\n");
////    fclose(fp);
//    Py_XDECREF(pmodule);
//    Py_XDECREF(pfunc);
//    Py_XDECREF(pArgs);
//    Py_XDECREF(pResult);
//
//    pthread_exit(NULL);
}