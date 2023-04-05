#include <postgres.h>
#include <nodes/nodes.h>
#include <nodes/pg_list.h>
#include <nodes/parsenodes.h>
#include <lib/stringinfo.h>
#include <Python.h>


void
query_to_string(Query *query);
void
query_by_python(char* attrs, char* table, char* where_clause);


void
query_by_python(char* attrs, char* table, char* where_clause){
    // Initialize Python interpreter
    fprintf(stderr, "Start query function.\n");
    Py_Initialize();

    // Build the name object for the module and function
    PyRun_SimpleString("import sys");
    PyRun_SimpleString("sys.path.append('/var/lib/postgresql/2.10.1_tsdb/src/PythonFile')");
    PyRun_SimpleString("print('Query start!')");
    PyObject* moduleName = PyUnicode_FromString("query");
    PyObject* functionName = PyUnicode_FromString("query");
    fprintf(stderr, "Start query function.\n");
    // Import the module
    PyObject* module = PyImport_Import(moduleName);
    if (module == NULL) {
        PyErr_Print();
        fprintf(stderr, "Failed to import module.\n");
    }

    // Build the argument list
    PyObject* args = PyTuple_New(3);
    PyTuple_SetItem(args, 0, PyUnicode_FromString(attrs));
    PyTuple_SetItem(args, 1, PyUnicode_FromString(table));
    PyTuple_SetItem(args, 2, PyUnicode_FromString(where_clause));

    // Call the function and get the result
    PyObject* result = PyObject_CallObject(PyObject_GetAttrString(module, "query"), args);
    if (result == NULL) {
        PyErr_Print();
        fprintf(stderr, "Failed to call function.\n");
    }

    // Print the result
    printf("Result: %s\n", PyUnicode_AsUTF8(result));

    // Clean up
    Py_DECREF(args);
    Py_DECREF(result);
    Py_DECREF(moduleName);
    Py_DECREF(functionName);
    Py_DECREF(module);

    // Shut down Python interpreter
    Py_Finalize();

}

void
query_to_string(Query *query)
{
//    fprintf(stderr, "Query to string start!!!!!!\n");
    // init attr_name
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

    if (query->jointree != NULL && query->jointree->quals != NULL)
    {
//        appendStringInfoString(&buf, " WHERE ");
        Node *quals = query->jointree->quals;
        char *quals_str = nodeToString(quals);
        appendStringInfoString(&where_part, quals_str);
        pfree(quals_str);
    }

    char *where_part_str = where_part.data;

    fprintf(stderr, "Attribute names: %s\n-------------------------\n", attr_name_str);
    fprintf(stderr, "Table names: %s\n-------------------------\n", table_name_str);
    fprintf(stderr, "Where part: %s\n-------------------------\n", where_part_str);

    query_by_python(attr_name_str, table_name_str, where_part_str);

}

