#include <postgres.h>
#include <nodes/nodes.h>
#include <nodes/pg_list.h>
#include <nodes/parsenodes.h>
#include <lib/stringinfo.h>
#include <Python.h>



void
query_to_string(Query* query);
void
query_by_python(char* attrs, char* table, char* where_clause);


void
query_by_python(char* attrs, char* table, char* where_clause){

    // Initialize Python interpreter
//    Py_Initialize();
    fprintf(stderr, "Start query function.\n");
    // Build the name object for the module and function
    PyRun_SimpleString("import sys");
    PyRun_SimpleString("sys.path.append('/var/lib/postgresql/2.10.1_tsdb/src/PythonFile')");


    // Import the module
    PyObject* pmodule = PyImport_ImportModule("query");
    if (!pmodule)
    {
        fprintf(stderr, "cannot find query.py\n");
//        return -1;
    }
    else
    {
        fprintf(stderr, "PyImport_ImportModule success\n");
    }

    PyObject *pfunc = PyObject_GetAttrString(pmodule, "query");
    if (!pfunc)
    {
        fprintf(stderr, "Cannot find func\n");
        Py_XDECREF(pmodule);
    }
    else
    {
        fprintf(stderr, "PyObject_GetAttrString success\n");
    }

    // Build the argument list
    PyObject* pArgs = PyTuple_New(3);
    PyTuple_SetItem(pArgs, 0, PyUnicode_FromString(attrs));
    PyTuple_SetItem(pArgs, 1, PyUnicode_FromString(table));
    PyTuple_SetItem(pArgs, 2, PyUnicode_FromString(where_clause));

    // Call the function and get the result
    PyObject *pResult = PyObject_CallObject(pfunc, pArgs);

    fprintf(stderr, "Query finish\n");
    Py_XDECREF(pmodule);
    Py_XDECREF(pfunc);
    Py_XDECREF(pArgs);
    Py_XDECREF(pResult);

//    Py_Finalize();


}

void
query_to_string(Query* query)
{

//    fprintf(stderr, "Thread detach\n");
    StringInfoData attr_name;
    initStringInfo(&attr_name);

    // init table_name
    StringInfoData table_name;
    initStringInfo(&table_name);

    // init where condition
    StringInfoData where_part;
    initStringInfo(&where_part);

//    appendStringInfoString(&buf, "SELECT ");
    ListCell *lc;
    foreach (lc, query->targetList)
    {
        TargetEntry *te = (TargetEntry *) lfirst(lc);
        appendStringInfoString(&attr_name, te->resname);
        ListCell *next = lnext(query->targetList, lc);
        if (next != NULL) {
            appendStringInfoString(&attr_name, ",");
        }
    }
    fprintf(stderr, "Finish adding attribute names!!!!!\n");
    char *attr_name_str = attr_name.data;

    RangeTblEntry *rte = (RangeTblEntry *) linitial(query->rtable);
    if(rte == NULL){
        return;
    }
    fprintf(stderr, "Actual query starts!!\n");
    appendStringInfoString(&table_name, rte->eref->aliasname);

    char *table_name_str = table_name.data;
    char *where_part_str;
    if (query->jointree != NULL && query->jointree->quals != NULL)
    {
//        appendStringInfoString(&buf, " WHERE ");
        Node *quals = query->jointree->quals;
        char *quals_str = nodeToString(quals);
        appendStringInfoString(&where_part, quals_str);
        pfree(quals_str);
        where_part_str = where_part.data;
    }




    fprintf(stderr, "Attribute names: %s\n-------------------------\n", attr_name_str);
    fprintf(stderr, "Table names: %s\n-------------------------\n", table_name_str);
    fprintf(stderr, "Where part: %s\n-------------------------\n", where_part_str);

    query_by_python(attr_name_str, table_name_str, where_part_str);

}

