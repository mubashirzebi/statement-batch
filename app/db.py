import threading

import oracledb


_oracle_init_lock = threading.Lock()
_oracle_initialized = False


def _initialize_oracle_client(config, logger=None):
    global _oracle_initialized
    if _oracle_initialized:
        return
    with _oracle_init_lock:
        if _oracle_initialized:
            return
            
        if not config.oracle_client_lib_dir:
            if logger:
                logger.info("Oracle client library directory not provided; proceeding in THIN mode")
            _oracle_initialized = True
            return

        if logger:
            logger.info(
                "initializing Oracle client in THICK mode from %s",
                config.oracle_client_lib_dir,
            )
        oracledb.init_oracle_client(lib_dir=str(config.oracle_client_lib_dir))
        _oracle_initialized = True


def create_pool(config, credentials, logger=None):
    _initialize_oracle_client(config, logger)
    mode = "THICK" if config.oracle_client_lib_dir else "THIN"
    if logger:
        logger.info("creating Oracle pool in %s mode", mode)
    return oracledb.create_pool(
        user=credentials.username,
        password=credentials.password,
        dsn=credentials.dsn,
        min=config.db_pool_min,
        max=config.db_pool_max,
        increment=config.db_pool_increment,
        getmode=oracledb.POOL_GETMODE_WAIT,
    )


def run_db_checks(pool, config):
    package_owner, package_name = _split_qualified_name(config.package_name)
    sequence_owner, sequence_name = _split_qualified_name(config.doc_sequence)

    with pool.acquire() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM dual")
            dual_value = cursor.fetchone()[0]

            if sequence_owner:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                      FROM all_sequences
                     WHERE sequence_owner = :owner
                       AND sequence_name = :sequence_name
                    """,
                    owner=sequence_owner,
                    sequence_name=sequence_name,
                )
            else:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                      FROM user_sequences
                     WHERE sequence_name = :sequence_name
                    """,
                    sequence_name=sequence_name,
                )
            sequence_count = cursor.fetchone()[0]

            if package_owner:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                      FROM all_objects
                     WHERE owner = :owner
                       AND object_name = :object_name
                       AND object_type = 'PACKAGE'
                    """,
                    owner=package_owner,
                    object_name=package_name,
                )
            else:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                      FROM user_objects
                     WHERE object_name = :object_name
                       AND object_type = 'PACKAGE'
                    """,
                    object_name=package_name,
                )
            package_count = cursor.fetchone()[0]

    return {
        "dual_value": dual_value,
        "sequence_found": bool(sequence_count),
        "sequence_name": config.doc_sequence,
        "package_found": bool(package_count),
        "package_name": config.package_name,
    }


def _split_qualified_name(value):
    parts = [part.strip().upper() for part in value.split(".", 1)]
    if len(parts) == 2:
        return parts[0], parts[1]
    return None, parts[0]
