"""
models.py — Datos de ejemplo para la tabla SCD

Replica exactamente la tabla del enunciado:
person_id | name            | state | city    | valid_from | valid_to
----------|-----------------|-------|---------|------------|----------
1         | John Doe        | TX    | Houston | 2020-01-01 | 2022-07-25
1         | John Doe        | TX    | Dallas  | 2022-07-25 | 2023-08-19
1         | John Doe        | TX    | null    | 2023-08-19 | null
2         | Richard Smith   | CA    | San...  | 2022-04-12 | null
3         | Max Mustermann  | CA    | null    | 2000-07-22 | null
"""

SCD_RECORDS = [
    # John Doe — 3 registros, ciudad progresa Houston → Dallas → null
    {"person_id": 1, "name": "John Doe",        "state": "TX", "city": "Houston", "valid_from": "2020-01-01", "valid_to": "2022-07-25"},
    {"person_id": 1, "name": "John Doe",        "state": "TX", "city": "Dallas",  "valid_from": "2022-07-25", "valid_to": "2023-08-19"},
    {"person_id": 1, "name": "John Doe",        "state": "TX", "city": None,      "valid_from": "2023-08-19", "valid_to": None},

    # Richard Smith — 1 registro, ciudad San (truncado en enunciado), valid_to null
    {"person_id": 2, "name": "Richard Smith",   "state": "CA", "city": "San",     "valid_from": "2022-04-12", "valid_to": None},

    # Max Mustermann — 1 registro, ciudad null desde el inicio
    {"person_id": 3, "name": "Max Mustermann",  "state": "CA", "city": None,      "valid_from": "2000-07-22", "valid_to": None},
]
