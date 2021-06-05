/***
Crear usuario tallerjson para la solucion del segundo punto
*/
alter session set "_ORACLE_SCRIPT"=true;
CREATE USER tallerjson IDENTIFIED BY oracle;

/***
Acceso a la carpeta Temp
*/
--CREATE OR REPLACE DIRECTORY UTL_DIR_T AS 'C:\Temp';
--GRANT read, write ON DIRECTORY UTL_DIR_T TO tallerjson;
select directory_name, directory_path from all_directories;


/******************************************************************
******************************************************************/
/****
Crear tablas para el desarrollo del ejercicio
*/
CREATE TABLE person(
    id_person VARCHAR2(5),
    first_name VARCHAR2(45),
    last_name VARCHAR2(45),
    salary NUMBER(8),
    address VARCHAR(45),
    city_id_city NUMBER(4),
    CONSTRAINT person_pk_idperson PRIMARY KEY (id_person)
)

CREATE TABLE city(
    id_city NUMBER(4),
    city VARCHAR2(45),
    state_id_state NUMBER(4),
    CONSTRAINT city_pk_idcity PRIMARY KEY (id_city)
)

CREATE TABLE state(
    id_state NUMBER(4),
    state VARCHAR2(45),
    CONSTRAINT state_pk_idstate PRIMARY KEY (id_state)
)

alter table person add(
	constraint CITY_FK_IDCITY foreign key (city_id_city) references city(id_city)
);

alter table city add(
	constraint STATE_FK_IDSTATE foreign key (state_id_state) references state(id_state)
);

CREATE TABLE json_documents (
 id NUMBER,
 datajson CLOB,
 CONSTRAINT json_documents_pk PRIMARY KEY (id),
 CONSTRAINT json_documents_json_chk CHECK (datajson IS JSON)
);


/******************************************************************
******************************************************************/
/***
procedimiento savejson que se encarga de recibir el json obtenido
desde el archivo datos.json e insertarlo posteriormente en la tabla
json_documents.
*/
CREATE OR REPLACE PROCEDURE savejson(p_json clob)
IS
BEGIN
    INSERT INTO json_documents values (2,p_json);
END savejson;


/******************************************************************
******************************************************************/
/****
bloque anonimo leer el archivo datos.json, recorrer las líneas del
archivo, guardar en una variable todo el contenido del json y enviarlo al
procedimiento savejson para ser guardado.
*/
SET SERVEROUTPUT ON
declare
v_archivo UTL_FILE.FILE_TYPE;
v_linea varchar2(100);
v_getcont_json clob;
v_contenido_json clob;
begin
v_archivo:= UTL_FILE.FOPEN('UTL_DIR_T','datos2.json','R');
    loop
        begin
            v_getcont_json:=v_getcont_json||v_linea;
            utl_file.get_line(v_archivo,v_linea);
            DBMS_OUTPUT.PUT_LINE (v_linea);
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
            utl_file.fclose(v_archivo);
            EXIT;
    end;
    end loop; 
    v_contenido_json:=v_getcont_json;
    savejson(v_contenido_json);
utl_file.fclose(v_archivo); 
end;

/******************************************************************
******************************************************************/
/***
Funcion para obtener el maximo id registrado de los estados
*/
CREATE OR REPLACE FUNCTION calc_idstate
RETURN NUMBER
IS
v_maxid_state NUMBER;
BEGIN 
    SELECT MAX(id_state)
    INTO v_maxid_state
    FROM STATE;
    IF v_maxid_state IS NULL THEN
        v_maxid_state:=0;
    END IF;
    RETURN v_maxid_state;
END calc_idstate;

/***
Funcion para obtener el maximo id registrado de las ciudades
*/
CREATE OR REPLACE FUNCTION calc_idcity
RETURN NUMBER
IS
v_maxid_city NUMBER;
BEGIN 
    SELECT MAX(id_city)
    INTO v_maxid_city
    FROM city;
    IF v_maxid_city IS NULL THEN
        v_maxid_city:=0;
    END IF;
    RETURN v_maxid_city;
END calc_idcity;


/******************************************************************
******************************************************************/
/**
procedimiento para insertar los estados.
*/
CREATE OR REPLACE PROCEDURE insert_state(p_state VARCHAR2)
is
v_state varchar2(45);
begin
    SELECT state
    into v_state
    from state
    where state = p_state;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
        insert into state values(calc_idstate+1,p_state);
end insert_state;

/******************************************************************
******************************************************************/
/**
procedimiento para insertar las ciudades.
*/
CREATE OR REPLACE PROCEDURE insert_city(p_state VARCHAR2,p_city VARCHAR2)
is
v_idstate number(4);
v_city varchar2(45);
begin
    SELECT id_state
    into v_idstate
    from state
    where state = p_state;
    SELECT city
    into v_city
    from city
    where city = p_city;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
        insert into city values(calc_idcity+1,p_city,v_idstate);
end insert_city;
/******************************************************************
******************************************************************/
/**
procedimiento para insertar personas
*/
CREATE OR REPLACE PROCEDURE insert_person(p_idperson VARCHAR2, p_f_name VARCHAR2,P_last_name VARCHAR2,p_salary VARCHAR2, p_address VARCHAR2,p_city VARCHAR2)
is
v_idcity number(4);
begin
    SELECT id_city
    INTO v_idcity
    FROM city
    WHERE city = p_city;
    insert into person values(p_idperson,p_f_name,P_last_name,p_salary,p_address,v_idcity);
end insert_person;

/******************************************************************
******************************************************************/
/**
Bloque anonimo que corre el proyecto
*/
DECLARE
v_contenidojson clob;
v_top_obj JSON_OBJECT_T;
v_top_row_obj JSON_OBJECT_T;
v_person_obj JSON_OBJECT_T;
v_ubication_obj JSON_OBJECT_T;
v_row_arr JSON_ARRAY_T;
v_tamanio number;
v_ciudad varchar2(50);
BEGIN
    SELECT datajson
    INTO v_contenidojson
    FROM json_documents
    WHERE id = 1;
    v_top_obj := JSON_OBJECT_T(v_contenidojson);
    v_top_row_obj:=v_top_obj.get_object('person');
    v_row_arr := v_top_row_obj.get_array('row');
    v_tamanio:= v_row_arr.get_size;
    FOR i IN 0 .. v_tamanio-1 LOOP
        v_person_obj := TREAT(v_row_arr.get(i) AS JSON_OBJECT_T);
        v_ubication_obj:= v_person_obj.get_object('address_city');
         insert_state(v_ubication_obj.get_string('State'));
         insert_city(v_ubication_obj.get_string('State'),v_ubication_obj.get_string('City'));
         insert_person(v_person_obj.get_string('id'),v_person_obj.get_string('first_name'),v_person_obj.get_string('last_name'),v_person_obj.get_string('salary'),v_person_obj.get_string('address_number'),v_ubication_obj.get_string('City'));
    END LOOP;
END;


/******************************************************************
******************************************************************/

select p.id_person,p.first_name,p.last_name,p.salary,p.address,c.city,s.state
from person p,city c, state s
where p.city_id_city = c.id_city
and c.state_id_state = s.id_state;


DECLARE
v_contenidojson clob;
v_top_obj JSON_OBJECT_T;
v_person_obj JSON_OBJECT_T;
v_ubication_obj JSON_OBJECT_T;
v_row_arr JSON_ARRAY_T;
v_tamanio number;

v_ciudad varchar2(50);
BEGIN
    SELECT datajson
    INTO v_contenidojson
    FROM json_documents
    WHERE id = 2;
    
    v_top_obj := JSON_OBJECT_T(v_contenidojson);
    v_row_arr := v_top_obj.get_array('person');
    v_tamanio:= v_row_arr.get_size;
    FOR i IN 0 .. v_tamanio-1 LOOP
        v_person_obj := TREAT(v_row_arr.get(i) AS JSON_OBJECT_T);
        v_ubication_obj:= v_person_obj.get_object('address_city');
         DBMS_OUTPUT.put_line('index : ' || i);
         DBMS_OUTPUT.put_line('first_name : ' ||v_person_obj.get_string('first_name')||' '||v_person_obj.get_string('last_name')); 
         DBMS_OUTPUT.put_line('Estado : ' ||v_ubication_obj.get_string('State'));
         DBMS_OUTPUT.put_line('Ciudad : ' ||v_ubication_obj.get_string('City'));
         insert_state(v_ubication_obj.get_string('State'));
         --v_ciudad:=v_ubication_obj.get_string('City');
        --DBMS_OUTPUT.put_line(v_ciudad);
    END LOOP;
    --DBMS_OUTPUT.PUT_LINE (v_top_obj.to_string);
    --DBMS_OUTPUT.PUT_LINE (v_tamanio);
END;








