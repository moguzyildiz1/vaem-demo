/*
 *  Creates and initializes vaem dashboard related schemas.
 */
-- VAEM_Valve_Controller.sql --

-- VALVE schema --
CREATE TABLE IF NOT EXISTS valve (
    valve_id INTEGER PRIMARY KEY,
    status TEXT NOT NULL,
    opening_time INTEGER,
    closing_time INTEGER,
    is_selected BOOLEAN DEFAULT 0
);

-- VAEM_STATUS schema --
CREATE TABLE IF NOT EXISTS vaem_status (
    timestamp INTEGER DEFAULT (cast(strftime('%s', 'now') as integer)),
    status_flags TEXT
);

-- VAEM_ERRORS schema --
CREATE TABLE IF NOT EXISTS vaem_errors (
    error_id INTEGER PRIMARY KEY AUTOINCREMENT,
    error_description TEXT NOT NULL,
    timestamp INTEGER DEFAULT (cast(strftime('%s', 'now') as integer))
);

-- OPERATION_LOG schema --
CREATE TABLE IF NOT EXISTS operation_log (
    operation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    valve_id INTEGER,
    operation_name TEXT NOT NULL,
    timestamp INTEGER DEFAULT (cast(strftime('%s', 'now') as integer)),
    FOREIGN KEY (valve_id) REFERENCES valve(valve_id)
);

