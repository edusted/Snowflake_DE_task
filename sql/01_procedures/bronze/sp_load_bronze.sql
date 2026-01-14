CREATE OR REPLACE PROCEDURE UTILS.SP_LOAD_BRONZE(FILE_NAME VARCHAR)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    EXECUTE IMMEDIATE 'COPY INTO BRONZE.RAW_AIRLINE_DATA (
                            ROW_ID,
                            PASSENGER_ID,
                            FIRST_NAME,
                            LAST_NAME,
                            GENDER,
                            AGE,
                            NATIONALITY,
                            AIRPORT_NAME,
                            AIRPORT_COUNTRY_CODE,
                            COUNTRY_NAME,
                            AIRPORT_CONTINENT,
                            CONTINENTS,
                            DEPARTURE_DATE,
                            ARRIVAL_AIRPORT,
                            PILOT_NAME,
                            FLIGHT_STATUS,
                            TICKET_TYPE,
                            PASSENGER_STATUS
                       ) 
                       FROM @UTILS.RAW_STAGE/' || :FILE_NAME || ' 
                       FILE_FORMAT = (FORMAT_NAME = UTILS.CSV_FORMAT)
                       ON_ERROR = ''CONTINUE'''; 
                       
    INSERT INTO UTILS.AUDIT_LOG (TASK_NAME, STATUS, TARGET_TABLE, AFFECTED_ROWS)
    SELECT 'LOAD_BRONZE', 'SUCCESS', 'RAW_AIRLINE_DATA', COUNT(*) 
    FROM BRONZE.RAW_AIRLINE_DATA 
    WHERE LOAD_TIMESTAMP >= DATEADD(minute, -5, CURRENT_TIMESTAMP()); 
    
    COMMIT;
    
    RETURN 'Bronze Load Complete';
END;
$$;