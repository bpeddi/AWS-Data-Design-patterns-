--- following procedure inserts timestamp and string into public.etl_events tables
CREATE OR REPLACE PROCEDURE public.sp_etl_log_etl_event (
   p_event_message IN VARCHAR(65535)
)
AS
$$
DECLARE
  v_sql VARCHAR(65535) := '';
BEGIN
  RAISE NOTICE '[ETL App event message][%]' , p_event_message;
CREATE TABLE IF NOT EXISTS public.etl_events (
	    	event_ts timestamp not null ,event_message  VARCHAR(65535)
) backup yes diststyle even;

v_sql := v_sql || 'INSERT INTO public.etl_events (event_ts, event_message) '||CHR(13);
v_sql := v_sql || 'VALUES (''' || sysdate || ''','''  ||CHR(13);
v_sql := v_sql ||  p_event_message || ''')' || CHR(13);

RAISE NOTICE '[SQL Statement][%]', v_sql;
EXECUTE v_sql;

END;
$$ LANGUAGE plpgsql
;
