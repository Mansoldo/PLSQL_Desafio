-- PERGUNTA 3
-- CRIAÇÃO DA TABELA DE EVENTOS
CREATE TABLE EVENTS (
	EVENT_TYPE INTEGER NOT NULL,
	VALUE INTEGER NOT NULL,
	TIME TIMESTAMP NOT NULL,
	UNIQUE (EVENT_TYPE, TIME)
);

/

-- CRIAÇÃO DA TABELA TEMPORÁRIA PARA ARMAZENAR OS EVENTOS E SEUS VALORES
CREATE GLOBAL TEMPORARY TABLE TEMP_EVENT (
    EVENT_TYPE INTEGER NOT NULL,
	VALUE INTEGER NOT NULL	
);

/

-- CRIAÇÃO DA PROC
CREATE OR REPLACE PROCEDURE EVENTTYPE
IS
    V_EVENT EVENTS.EVENT_TYPE%TYPE;
    V_CONT INTEGER := 0;
    V_VALUE_OLDEST INTEGER := 0;
    V_VALUE_MIDDLE INTEGER := 0;
    V_RESPOSTA INTEGER := 0;    
    
    --CURSOR COLETANDO O EVENTO E A QUANTIDADE DE VEZES QUE APARECE, TENDO MAIS DE UMA APARIÇÃO
    CURSOR EVENTTPE IS
        SELECT EVENT_TYPE, COUNT(*) FROM EVENTS GROUP BY EVENT_TYPE HAVING COUNT(*) > 1;
        
        BEGIN
            OPEN EVENTTPE;            
                LOOP           
                    FETCH EVENTTPE INTO V_EVENT, V_CONT;     
                    
                    --SAÍDA QUANDO NÃO HOUVER MAIS REGISTRO
                    EXIT WHEN EVENTTPE%NOTFOUND;
                    
                    -- SE TIVER MAIS DE DOIS (ANTEPENULTIMO, PENULTIMO E ULTIMO)
                    IF V_CONT > 2 THEN

                    -- VALOR INTERMEDIARIO
                    WITH TABELA_1 AS (
                        SELECT
                        EVENT_TYPE,
                        VALUE,
                        RANK() OVER (ORDER BY TIME ASC) AS RANKED
                        FROM EVENTS
                        WHERE EVENT_TYPE = V_EVENT
                    )
                    SELECT VALUE INTO V_VALUE_MIDDLE FROM TABELA_1 WHERE RANKED = 2;
                    
                    -- VALOR MAIS ANTIGO
                    WITH TABELA_2 AS (
                        SELECT
                        EVENT_TYPE,
                        VALUE,
                        RANK() OVER (ORDER BY TIME ASC) AS RANKED
                        FROM EVENTS
                        WHERE EVENT_TYPE = V_EVENT
                    )                    
                    SELECT VALUE INTO V_VALUE_OLDEST FROM TABELA_2 WHERE RANKED = 1;                    
                
                	-- DIFERENÇA ENTRE ELES
                    V_RESPOSTA := V_VALUE_MIDDLE - V_VALUE_OLDEST;
                
                    ELSE  
                    -- VALOR ZERADO CASO HAJA APENAS DOIS REGISTROS
                        V_RESPOSTA := 0;
                    END IF;
                
                    INSERT INTO TEMP_EVENT 
                    SELECT V_EVENT, V_RESPOSTA FROM DUAL;                
                    
                END LOOP;
            CLOSE EVENTTPE;
        END;

/

--EXECUÇÃO DA PROC
SET SERVEROUTPUT ON

BEGIN
    EVENTTYPE;
END;

/

-- VALIDAÇÃO DA INSERÇÃO MA TABELA TEMPORARIA
SELECT * FROM TEMP_EVENT ORDER BY EVENT_TYPE ASC;