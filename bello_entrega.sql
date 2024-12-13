/**********************************************************************************
               Generación automatizada de registros de acuerdo a las
               		 especificaciones del municipio de Bello
              ----------------------------------------------------------
        begin           : 2024-10-23
        git sha         : :%H$
        copyright       : (C) 2024 by Leo Cardona (CEICOL SAS)
						  (C) 2024 by Juanita Rodríguez (CEICOL SAS)  
        email           : contacto@ceicol.com
                          dev.ceicol@ceicol.com
 ***************************************************************************/
/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License v3.0 as          *
 *   published by the Free Software Foundation.                            *
 *                                                                         *
 **********************************************************************************/

-- Fijar esquemas
set search_path to 	
	datos, 
    public; -- Esquema del modelo de captura en campo

--Revisar cuántos predios no tienen número de ficha (el campo nombre es null)
select * from cca_predio where nombre is null;


--Actualizar la tabla predio para asignar un serial a aquellos predios sin número de ficha
with serials as (
  select t_id, 
         ROW_NUMBER() over (order by t_id) + 8800000 as new_serial
  from cca_predio
  where nombre is null
)
update cca_predio
set nombre = new_serial::varchar
from serials
where cca_predio.t_id = serials.t_id;

--Borrar el esquema si ya existe
drop schema if exists resultados CASCADE;
--Crear el esquema para guardar los resultados
create schema resultados;

------------------------------------
--------- Crear registro 1 ---------
------------------------------------
select *  from cca_predio cp where substring(cp.numero_predial,7,1)='';

create table IF NOT EXISTS resultados.registro_1 as
select 
	1 as id_tabla,
	2024 as anio_resolucion,
	000 as resolucion,
	cp.nombre as nro_ficha,
	'088' as mpio,
	cp.numero_predial,
	case 
		when substring(cp.numero_predial,7,1)='1'
		then 1
		else 2
	end as sector,
	case 
		when substring(cp.numero_predial,7,1)='1'
		then 1
		when substring(cp.numero_predial,7,1)=''
		then 1
		else cast(substring(cp.numero_predial,7,1) as INTEGER)
	end as correg,   
	substring(cp.numero_predial,12,2) as barrio,
	substring(cp.numero_predial,14,4) as manz_vere,
	substring(cp.numero_predial,18,4) as predio,
	substring(cp.numero_predial,23,2) as edificio,
	substring(cp.numero_predial,25,2) as und_pred,
	case 
        when substring(cp.numero_predial,22,1) LIKE '2' THEN 1
        else 2
    end as informalidad,
	0 as nroanter,
	0 as mpio_ant,
	0 as sector_ant,
	0 as correg_ant,
	0 as barrio_ant,
	0 as manz_vere_ant,
	0 as predio_ant,
	0 as edificio_ant,
	0 as und_pred_ant,
	0 as mejora_ant,
	(select ilicode from cca_destinacioneconomicatipo where t_id = cp.destinacion_economica) destino_economico,
	case 
		when (select count(*) 
			from cca_direccion cd 
			where cd.cca_predio_direccion = cp.t_id) > 1
    	then (select complemento 
          from cca_direccion cd 
          where cd.cca_predio_direccion = cp.t_id and cd.es_direccion_principal is true)
    	else (select complemento 
          from cca_direccion cd 
          where cd.cca_predio_direccion = cp.t_id)
	end as direccion,
	2 as desplazado, 
	2 as novedad, 
	8 as clase_mutacion,
	DATE '2024-10-23' as fecha_inscripcion, 
	cp.numero_predial as nup,
	cp.coeficiente_copropiedad as coeficiente_copropiedad,
	'Actualizaciones' as tipo_tramite,
	'' as lleno
from cca_predio cp;

select * from cca_predio;

COPY resultados.registro_1 TO 'C:/Users/PC/Documents/CEICOL/REVISION BELLO/comuna_8/registro_1.csv' WITH CSV DELIMITER '|' HEADER;

select * from resultados.registro_1; -- 96128
select * from cca_predio; -- 96128

------------------------------------
--------- Crear registro 2 ---------
------------------------------------

create table IF NOT EXISTS resultados.registro_2 as
select 
	2 as id_tabla,
	cp.nombre as nro_ficha,
	2025 as vigencia_avaluo,
	case 
		when substring(cp.numero_predial,22,1)='9' 
		and substring(cp.numero_predial,23,8)='00000000'
		or substring(cp.numero_predial,22,1)='8'
		then ci.area_total_terreno_comun/10000
		when substring(cp.numero_predial,22,1)='9' and substring(cp.numero_predial,23,8)!='00000000'
		then cp.coeficiente_copropiedad*(ci.area_total_terreno_comun)/10000
		else (cp.area_catastral_terreno)/10000
	end as area_lote_comun,
	case 
		when substring(cp.numero_predial,22,1)='9' 
		and substring(cp.numero_predial,23,8)='00000000'
		or substring(cp.numero_predial,22,1)='8' 
		then ci.area_total_terreno_privada/10000
		when substring(cp.numero_predial,22,1)='9' and substring(cp.numero_predial,23,8)!='00000000'
		then cp.coeficiente_copropiedad*(ci.area_total_terreno_privada)/10000
		else (cp.area_catastral_terreno)/10000
	end as area_lote_privada,
	case 
	    when substring(cp.numero_predial,22,1) = '9' and substring(cp.numero_predial,23,8) = '00000000' then
	        case 
	            when ci.area_total_construida_comun is null 
	            then null
	            else ci.area_total_construida_comun
	        end
	    else
	        case 
	            when ta.area_construida is null or ta.area_construida = 0 or ta.area_privada_construida is null
	            then null
	            else 
		            case 
		            	when (ta.area_construida - ta.area_privada_construida) < 0 
		            	then null 
		            	else (ta.area_construida - ta.area_privada_construida)
		            end  					
	        end
	end as area_construida_comun,
	case 
		when substring(cp.numero_predial,22,1)='9' and substring(cp.numero_predial,23,8)='00000000'
		then ci.area_total_construida_privada
		else ta.area_privada_construida
	end as area_construida_privada,
	case
		when  substring(cp.numero_predial,22,1)='0'
		then cp.avaluo_catastral_terreno
		else 0
	end as valor_terreno_comun,
	case
		when substring(cp.numero_predial,22,1)='0'
		then cp.avaluo_catastral_terreno
		else 0
	end as valor_terreno_privado,
	case
		when substring(cp.numero_predial,22,1)='0'
		then (cp.avaluo_catastral-cp.avaluo_catastral_terreno)
		else 1
	end as valor_construccion_comun,
	case
		when substring(cp.numero_predial,22,1)='8'
		then (cp.avaluo_catastral-cp.avaluo_catastral_terreno)
		else 2
	end as valor_construccion_privado,
	cp.avaluo_catastral as avaluo,
	3 as autoestimacion,
	'' as campo
from cca_predio cp
join (select nombre,
     		  	SUM(cc.area_construida) AS area_construida,
     		 	SUM(cc.area_privada_construida) AS area_privada_construida
			from cca_predio cp
			left join cca_unidadconstruccion cu ON cu.predio = cp.t_id
			join cca_caracteristicasunidadconstruccion cc ON cc.t_id = cu.caracteristicasunidadconstruccion
			group by nombre) ta on ta.nombre=cp.nombre 
left join cca_informacionph ci on ci.cca_predio=cp.t_id
left join cca_unidadconstruccion cu on cu.predio=cp.t_id
join cca_caracteristicasunidadconstruccion cc on cc.t_id=cu.caracteristicasunidadconstruccion;

select id_operacion, area_construida, area_privada_construida from cca_predio;

COPY resultados.registro_2 TO 'C:/Users/PC/Documents/CEICOL/REVISION BELLO/comuna_8/registro_2.csv' WITH CSV DELIMITER '|' HEADER;

select area_construida_comun, area_construida_privada from resultados.registro_2 where area_construida_comun < 0;

select * from resultados.registro_2 where area_lote_comun < 0;
select * from resultados.registro_2 where area_lote_privada < 0;
select * from resultados.registro_2 where area_construida_comun < 0;
select * from resultados.registro_2 where area_construida_privada < 0;

select cp.id_operacion, cp.nombre as numero_ficha, cc.identificador, cc.area_construida, cc.area_privada_construida from cca_predio cp
join cca_unidadconstruccion cu on cp.t_id=cu.predio
join cca_caracteristicasunidadconstruccion cc on cc.t_id=cu.caracteristicasunidadconstruccion
where cc.area_construida = 0;
------------------------------------
--------- Crear registro 3 ---------
------------------------------------

create table IF NOT EXISTS resultados.registro_3 as
select 
	3 as id_tabla,
	cp.numero_predial as nro_ficha,
	2 as novedad,
	(select ilicode from cca_interesadodocumentotipo cit where cit.t_id = ci.tipo_documento ) as tipo_doc, 
	ci.documento_identidad as documento, 
	1 as tipo_propiet,
	ci.primer_apellido as pri_apellido,
	ci. segundo_apellido as seg_apellido,
	concat(ci.primer_nombre, ' ', ci.segundo_nombre) as nombres,
	ci.razon_social as razon_social, 
	(select ilicode from cca_fuenteadministrativatipo cft where cft.t_id=cf.tipo) as tipo_documento_fuente, 
	case 
		when substring(cp.numero_predial,22,1)='2'
		then 'Notaria 0'
		else cf.ente_emisor
	end as notaria,
	cf.numero_fuente as escritura,
	cf.fecha_documento_fuente as fecha_escritura,
	cd.cuota_participacion as derecho,
	'2' as litigio, 
	0 as porc_litigio, 
	1 as gravable, 
	cp.codigo_orip as circulo,
	cp.matricula_inmobiliaria, 
	'0' as tomo, 
	'0' as libro, 
	'0' as codigo_fidecomiso,
	'' as nombre_fidecomiso, 
	'' as campo
from cca_predio cp 
join cca_derecho cd on cd.predio=cp.t_id
join cca_interesado ci on ci.t_id=cd.interesado
join cca_fuenteadministrativa cf on cf.derecho=cd.t_id;

COPY resultados.registro_3 TO 'C:/Users/PC/Documents/CEICOL/REVISION BELLO/comuna_8/registro_3.csv' WITH CSV DELIMITER '|' HEADER;

SELECT DISTINCT fecha_escritura
FROM resultados.registro_3
WHERE CAST(split_part(fecha_escritura::TEXT, '-', 1) AS INTEGER) > 2024;

SELECT DISTINCT fecha_documento_fuente
FROM cca_fuenteadministrativa
WHERE CAST(split_part(fecha_documento_fuente::TEXT, '-', 1) AS INTEGER) > 2024;

select distinct fecha_documento_fuente from cca_fuenteadministrativa;


------------------------------------
--------- Crear registro 5 ---------
------------------------------------

--Se verifican los campos de unidad de construcción
select * from cca_unidadconstruccion;

-- Se crea el campo id_uconstruccion (si no existe)   
ALTER TABLE cca_unidadconstruccion
ADD COLUMN id_uconstruccion INTEGER;

-- Se agrega el identificador de la construcción (secuencial por predio)
with row_numbers_uconstruccion as (
    select cu.t_id,
           row_number() over (partition by cp.nombre) as id_uconstruccion
    from cca_unidadconstruccion cu
    join cca_predio cp on cu.predio = cp.t_id
)
update cca_unidadconstruccion
set id_uconstruccion = rnu.id_uconstruccion
from row_numbers_uconstruccion rnu 
where cca_unidadconstruccion.t_id = rnu.t_id;

-- Se crea la tabla del registro 5

create table IF NOT EXISTS resultados.registro_5 as
select 
	5 as id_tabla,
	cp.nombre as nro_ficha,
	id_uconstruccion as id_uconstruccion,
	cc.area_construida as area_constr,
	ccc.total_calificacion as puntos, 
	(select ilicode from cca_usouconstipo cuc where cc.uso=cuc.t_id) as descrip_uso, 
	(select ilicode from cca_unidadconstrucciontipo cun where cun.t_id=cc.tipo_unidad_construccion) as tipo_constr,
	(select ilicode from cca_armazontipo ca where ca.t_id=ccc.armazon) as armazon,
	(select ilicode from cca_murostipo cm where cm.t_id=ccc.muros) as muros, 
	(select ilicode from cca_cubiertatipo cct where cct.t_id=ccc.cubierta) as cubierta, 
	(select ilicode from cca_estadoconservaciontipo ce where ce.t_id=ccc.conservacion_cubierta) as conservacion_cubierta, 
	(select ilicode from cca_fachadatipo cf where cf.t_id=ccc.fachada) as fachada,
	(select ilicode from cca_cubrimientomurostipo ccm where ccm.t_id=ccc.cubrimiento_muros) as cubrimiento_muros, 
	(select ilicode from cca_pisotipo cpt where cpt.t_id=ccc.piso) as pisos, 
	(select ilicode from cca_estadoconservaciontipo cet where cet.t_id=ccc.conservacion_acabados) as conservacion_acabados,
	(select ilicode from cca_tamaniobaniotipo ctb where ctb.t_id=ccc.tamanio_banio) as tamanio_banio,
	(select ilicode from cca_enchapebaniotipo ceb where ceb.t_id=ccc.enchape_banio) as enchapes_banio, 
	(select ilicode from cca_mobiliariobaniotipo cmb where cmb.t_id=ccc.mobiliario_banio) as mobiliario_banio,
	(select ilicode from cca_estadoconservaciontipo cec where cec.t_id=ccc.conservacion_cocina) as conservacion_cocina, 
	0 as acueducto, 
	0 as alcantarillado, 
	0 as energia, 
	0 as telefono, 
	0 as gas, 
	0 as fibra_optica, 
	'' as tipologia,
	'' as campo
	from cca_predio cp 
join cca_unidadconstruccion cu on cu.predio=cp.t_id
join cca_caracteristicasunidadconstruccion cc on cc.t_id=cu.caracteristicasunidadconstruccion 
left join cca_calificacionconvencional ccc on ccc.cca_caracteristicasunidadconstruccion=cc.t_id; 

COPY resultados.registro_5 TO 'C:/Users/PC/Documents/CEICOL/REVISION BELLO/comuna_8/registro_5.csv' WITH CSV DELIMITER '|' HEADER;

------------------------------------
--------- Crear registro 7 ---------
------------------------------------

create table IF NOT EXISTS resultados.registro_7 as
select 
	7 as id_tabla,
	cp.numero_predial as npn,
	substring(cp.numero_predial,18,4) as etiqueta,
	ct.geometria as geometria
from cca_predio cp
left join cca_terreno ct on ct.predio=cp.t_id;

COPY resultados.registro_7 TO 'C:/Users/PC/Documents/CEICOL/REVISION BELLO/comuna_8/registro_7.csv' WITH CSV DELIMITER '|' HEADER;

------------------------------------
--------- Crear registro 8 ---------
------------------------------------

create table IF NOT EXISTS resultados.registro_8 as
select 
	8 as id_tabla,
	cp.numero_predial as npn,
	cu.id_uconstruccion,
	cu.planta_ubicacion,
	(select ilicode from cca_construccionplantatipo cc where cc.t_id=cu.tipo_planta) as planta_tipo,
	'' as etiqueta,
	case
		when cu.planta_ubicacion=1 or cu.planta_ubicacion=0
		then 'En_rasante'
		when cu.planta_ubicacion>1
		then 'En_Vuelo'
		when cu.planta_ubicacion<1
		then 'En_Subsuelo'
		else 'Otro'
	end	as relacion_superficie,
	cu.geometria as geometria
from cca_predio cp
left join cca_unidadconstruccion cu on cu.predio=cp.t_id;

COPY resultados.registro_8 TO 'C:/Users/PC/Documents/CEICOL/REVISION BELLO/comuna_8/registro_8.csv' WITH CSV DELIMITER '|' HEADER;


DO $$
DECLARE
    table_name text;
    file_path text;
BEGIN
    -- Iterar sobre todas las tablas en el esquema "resultados"
    FOR table_name IN
        SELECT tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname = 'resultados'
    LOOP
        -- Generar el path dinámico para cada tabla
        file_path := format('C:/Users/PC/Documents/CEICOL/ETLS/BELLO/comuna_8/%s.csv', table_name);

        -- Ejecutar la sentencia COPY para la tabla actual
        EXECUTE format(
            'COPY resultados.%I TO %L WITH CSV DELIMITER ''|'' HEADER',
            table_name,
            file_path
        );
    END LOOP;
END $$;


