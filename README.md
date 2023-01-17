# prueba_globant

## Load History
Script en Python en cargado de realizar la carga historica de las tablas jobs, departments y hired_employees

```bash
python3 load_history.py [jobs|departments|hired_employees]
```

## Api Load Data
Api construida en Flask que inserta en la base de datos los registros enviados al servicio load_data

```
http://localhost:4000/load_data
```

### Ejemplo payload Jobs

```json
{
    "data": [ {"id": 1899, "job": "Hola d"},
              {"id": 1900, "job": "Encargado d"},
              {"id": null, "job": "Auxiliar d"},
              {"id": 989, "job": null},
              {"id": 873}
            ],
    "table": "jobs"
}
```


### Ejemplo payload Departments
```json
{
    "data": [ {"id": 1910, "department": "Hola d"},
              {"id": 1911, "department": "Encargado d"},
              {"id": null, "department": "Auxiliar d"},
              {"id": 1890, "department": "Otro"},
              {"id": 873}
            ],
    "table": "departments"
}
```

### Ejemplo payload Hired Employees
```json
{
    "data": [ {"id": 2217, "name": "Luis", "datetime": "2023-03-09 22:18:26", "department_id": 12, "job_id": 34},
    {"id": 39440}
            ],
    "table": "hired_employees"
}
```

## Backup Restore
Script encargado de realizar resaldo de las tablas jobs, departments y hired_employees, además de realizar también la restauracion de los respaldos

```bash
   python3 data_back_up.py [jobs|departments|hired_employees] [back_up|restore]
```
