#include <postgres.h>
#include <nodes/nodes.h>
#include <nodes/pg_list.h>
#include <nodes/parsenodes.h>
#include <lib/stringinfo.h>
#include <Python.h>
#include <tcop/deparse_utility.h>
#include <catalog/namespace.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <parser/parse_node.h>



void
trans_parameter(char* str1, char* str2, char* str3);

void
query_to_string(Query *query);



void
trans_parameter(char* str1, char* str2, char* str3){
    // Initialize Python interpreter
    Py_Initialize();

    // Build the name object for the module and function
    PyObject* moduleName = PyUnicode_FromString("query");
    PyObject* functionName = PyUnicode_FromString("query");

    // Import the module
    PyObject* module = PyImport_Import(moduleName);
    if (module == NULL) {
        PyErr_Print();
        fprintf(stderr, "Failed to import module.\n");
//        return 1;
    }

    // Build the argument list
    PyObject* args = PyTuple_New(3);
    PyTuple_SetItem(args, 0, PyUnicode_FromString(str1));
    PyTuple_SetItem(args, 1, PyUnicode_FromString(str2));
    PyTuple_SetItem(args, 2, PyUnicode_FromString(str3));

    // Call the function and get the result
    PyObject* result = PyObject_CallObject(PyObject_GetAttrString(module, "query"), args);
    if (result == NULL) {
        PyErr_Print();
        fprintf(stderr, "Failed to call function.\n");
//        return 1;
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
    fprintf(stderr, "Query to string start!!!!!!\n");
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
    fprintf(stderr, "Finish creating variables!!!!!\n");
    ListCell *lc;
    foreach (lc, query->targetList)
    {
        TargetEntry *te = (TargetEntry *) lfirst(lc);
        appendStringInfoString(&attr_name, te->resname);
        ListCell *next = lnext(query->targetList, lc);
        if (next != NULL) {
            fprintf(stderr, "Count\n");
            appendStringInfoString(&attr_name, ",");
        }
    }
    fprintf(stderr, "Finish adding attribute names!!!!!\n");
    char *attr_name_str = attr_name.data;
    fprintf(stderr, "Attribute names: %s\n-------------------------\n", attr_name_str);

    RangeTblEntry *rte = (RangeTblEntry *) linitial(query->rtable);
    if(rte == NULL){
        return;
    }
    appendStringInfoString(&table_name, rte->eref->aliasname);

    char *table_name_str = table_name.data;
    fprintf(stderr, "Table names: %s\n-------------------------\n", table_name_str);


    char *where_clause_str;
    if (query->jointree != NULL && query->jointree->quals != NULL)
    {
        ParseState *pstate;
        // 创建一个新的 ParseState 以传递给 process_where_clause
        pstate = make_parsestate(NULL);
        Node *quals = query->jointree->quals;
        where_clause_str = deparse_expression(quals, pstate, false, false);
        // 释放 ParseState 结构体
        free_parsestate(pstate);
//        char *quals_str = nodeToString(quals);
//        appendStringInfoString(&where_part, quals_str);
//        pfree(quals_str);
    }

//    char *where_part_str = where_part.data;
    fprintf(stderr, "Where part: %s\n-------------------------\n", where_clause_str);
//    if (query->limitCount > 0)
//        appendStringInfo(&buf, " LIMIT %d", query->limitCount);

//    char *result = buf.data;
//    char *attr_name_str = attr_name.data;
//    char *table_name_str = table_name.data;
//    char *where_part_str = where_part.data;
//    fprintf(stderr, "Query:\n%s\n%s\n%s\n--------------------------\n", attr_name_str, table_name_str, where_part_str);

//    trans_parameter(attr_name_str,table_name_str,where_part_str);

}