#include <Python.h>
#include <arrayobject.h>

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include "structmember.h"

#include "hdfs.h" 

typedef struct {
    PyObject_HEAD
    hdfsFS fs;
    hdfsFile file;
} pyhdfsFS;


staticforward PyTypeObject pyhdfsFSType;

PyObject *exception = NULL;

static void
pyhdfsFS_dealloc(pyhdfsFS* self)
{
    self->ob_type->tp_free((PyObject*)self);
}


/* constructor */
static PyObject *
pyhdfsFS_open(char* filepath,char mode)
{ 
    pyhdfsFS *object;
    object = PyObject_NEW(pyhdfsFS, &pyhdfsFSType);

    if (object != NULL)
    {
        // Parse HDFS information
        char* hostport = (char *)malloc(strlen(filepath)+1);
        char* buf = NULL;
        char* path;
        char* portStr;  
        char* host;
        int hdfsPort;
        int ret = 0;

        ret = sscanf(filepath, "hdfs://%s", hostport);
        host = strtok_r(hostport, ":", &buf);
        portStr = strtok_r(NULL, "/", &buf);
        ret = sscanf(portStr, "%d", &hdfsPort);

        object->fs = hdfsConnect(host, hdfsPort);

        if (!object->fs)
        {
            PyErr_SetString(exception, "Cannot connect to host");
            return exception;
        }

        path = (char*) malloc(strlen(buf)+2);
        path[0] = '/';
        memcpy(path+1,buf,strlen(buf));
        path[strlen(buf)+1] = '\0';

        if (mode == 'r')
        {
            if (hdfsExists(object->fs, path) == 0)
            {
                object->file = hdfsOpenFile(object->fs, path, O_RDONLY, 0, 0, 0);    
            }
            else 
            {
                PyErr_SetString(exception, "Cannot open file for read");
                return NULL;
            }
        }
        else if (mode == 'w')
        {
            if (hdfsExists(object->fs, path) == 0)
            {            
                PyErr_SetString(exception, "Cannot open file for write");
                return NULL;
            }             
            else 
               object->file = hdfsOpenFile(object->fs, path, O_WRONLY, 0, 0, 0);    
        }
        else
        {
            PyErr_SetString(exception, "Invalid mode");
            return NULL;
        }

        if(!object->file) 
        {
            PyErr_SetString(exception, "Cannot open file");
            return NULL;
        }        
    }
    return (PyObject *)object;
}

static PyObject *
pyhdfsFS_delete(char* filepath)
{ 
    // Parse HDFS information
    char* hostport = (char *)malloc(strlen(filepath)+1);
    char* buf = NULL;
    char* path;
    char* portStr;  
    char* host;
    int hdfsPort;
    int ret = 0;

    ret = sscanf(filepath, "hdfs://%s", hostport);
    host = strtok_r(hostport, ":", &buf);
    portStr = strtok_r(NULL, "/", &buf);
    ret = sscanf(portStr, "%d", &hdfsPort);

    hdfsFS fs = hdfsConnect(host, hdfsPort);
    if (!fs)
    {
        PyErr_SetString(exception, "Cannot connect to host");
        return exception;
    }

    path = (char*) malloc(strlen(buf)+2);
    path[0] = '/';
    memcpy(path+1,buf,strlen(buf));
    path[strlen(buf)+1] = '\0';

    if (hdfsExists(fs, path) == 0)
    {
        hdfsDelete(fs, path);
    }
    else 
    {
        PyErr_SetString(exception, "Cannot delete file");
        return NULL;
    }

    return Py_BuildValue("i",1);
}


static PyObject *
pyhdfsFS_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    pyhdfsFS *self;

    self = (pyhdfsFS *)type->tp_alloc(type, 0);

    return (PyObject *)self;
}

static int
pyhdfsFS_init(pyhdfsFS *self, PyObject *args, PyObject *kwds)
{
    return 0;
}

static PyObject *pyhdfsFS_close(pyhdfsFS *self) 
{   
    if (self->file != NULL)
    {
        hdfsCloseFile(self->fs, self->file);
    }
    hdfsDisconnect(self->fs);
    return Py_BuildValue("i",1);
}

static PyObject *pyhdfsFS_write(pyhdfsFS *self, PyObject *args) 
{
    int i,n;

    PyObject *data;
    PyArrayObject *array;
    char *aptr;

    if (!PyArg_ParseTuple(args, "O", &data)) 
    {
        PyErr_SetString(exception, "Invalid data");
        return NULL;
    }

    array = (PyArrayObject *) PyArray_ContiguousFromObject(data, PyArray_CHAR, 0, 0);

    if (array == NULL)
    {
        PyErr_SetString(exception, "Cannot convert input data to array");
        return NULL;
    }

    // Compute Size of Array
    if(array->nd == 0)
        n = 1;
    else {
        n = 1;
        for(i=0;i<array->nd;i++) 
            n = n * array->dimensions[i];
    }

    aptr = array->data;

    tSize num_written_bytes = hdfsWrite(self->fs, self->file, (void*)aptr, n);

    tOffset currentPos = -1;
    if ((currentPos = hdfsTell(self->fs, self->file)) == -1) {
        return Py_BuildValue("i",0);
    }

    if (hdfsFlush(self->fs, self->file)) {
        return Py_BuildValue("i",0);
    }

    return Py_BuildValue("i",num_written_bytes);
}

static PyObject *pyhdfsFS_read(pyhdfsFS *self, PyObject *args) {

    int n;

    PyArrayObject *result;
    char *buffer;

    if (!PyArg_ParseTuple(args, "i", &n)) {
        return NULL;
    }
    
    int dim[1];
    dim[0] = n;
    result = (PyArrayObject *) PyArray_SimpleNew(1, dim, PyArray_CHAR);
    buffer = result->data;
    if (hdfsRead(self->fs, self->file, (void*)buffer, n) > 0)
        return PyArray_Return(result);
    else 
        return Py_None;
}

static PyMemberDef pyhdfsFS_members[] = {
    {NULL}  /* Sentinel */
};

static PyObject *pyhdfs_open(PyObject *self, PyObject *args) 
{
    pyhdfsFS *result = NULL;
    char mode;
    char *filepath;    

    if (!PyArg_ParseTuple(args, "sc", &filepath,&mode)) 
    {
        return NULL;
    }    

    result = (pyhdfsFS*) pyhdfsFS_open(filepath,mode);

    return (PyObject*) result;
}

static PyObject *pyhdfs_delete(PyObject *self, PyObject *args) 
{
    PyObject *result = NULL;
    char *filepath;    

    if (!PyArg_ParseTuple(args, "s", &filepath)) 
    {
        return NULL;
    }    

    result = pyhdfsFS_delete(filepath);

    return result;
}

static PyMethodDef pyhdfs_funcs[] = {
    {"open", (PyCFunction)pyhdfs_open, METH_VARARGS,
     "Open an hdfs file for read or write purpose"
    },
    {"delete", (PyCFunction)pyhdfs_delete, METH_VARARGS,
     "Delete an existing hdfs file"
    },
    {NULL}
};

static PyMethodDef pyhdfsFS_methods[] = {
    {"write", (PyCFunction)pyhdfsFS_write, METH_VARARGS,
     "Write data to an openned pyhdfsFS object"
    },
    {"read", (PyCFunction)pyhdfsFS_read, METH_VARARGS,
     "Read data from an openned pyhdfsFS object"
    },
    {"close", (PyCFunction)pyhdfsFS_close, METH_NOARGS,
     "Close an hdfs file"
    },
    {NULL}  /* Sentinel */
};

static PyTypeObject pyhdfsFSType = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "pyhdfsFS",                /*tp_name*/
    sizeof(pyhdfsFS),          /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)pyhdfsFS_dealloc, /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "pyhdfsFS objects",           /* tp_doc */
    0,		               /* tp_traverse */
    0,		               /* tp_clear */
    0,		               /* tp_richcompare */
    0,		               /* tp_weaklistoffset */
    0,		               /* tp_iter */
    0,		               /* tp_iternext */
    pyhdfsFS_methods,             /* tp_methods */
    pyhdfsFS_members,             /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)pyhdfsFS_init,      /* tp_init */
    0,                         /* tp_alloc */
    pyhdfsFS_new,                 /* tp_new */
};

void initpyhdfs(void)
{
    PyObject *m;

    if (PyType_Ready(&pyhdfsFSType) < 0)
        return;

    import_array();

    m = Py_InitModule3("pyhdfs", pyhdfs_funcs,
                   "Extension for Python to read/write to HDFS");

    if (m == NULL)
        return;

    Py_INCREF(&pyhdfsFSType);
    PyModule_AddObject(m, "pyhdfsFS", (PyObject *)&pyhdfsFSType);

    exception = PyErr_NewException("pyhdfs.error", NULL, NULL);
    Py_INCREF(exception);
    PyModule_AddObject(m, "error", exception);
}
