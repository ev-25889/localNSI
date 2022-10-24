--STUDENT_STATUS
CREATE TABLE public.student_status (
	id serial4 NOT NULL,
	external_id varchar(50) NOT NULL,
	gisscos_id varchar(50) NULL,
	status varchar(50) NOT NULL DEFAULT 'new'::character varying,
	datesynq timestamp NULL,
	responce text NULL,
	CONSTRAINT student_status_pkey PRIMARY KEY (id)
);

CREATE OR REPLACE FUNCTION public.update_student_status_from_student()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    mstr varchar(30);
    astr varchar(100);
    retstr varchar(254);
BEGIN
    IF TG_OP = 'UPDATE' THEN
		UPDATE student_status set "status" = 'to_del' WHERE student_status."external_id" =
		new."ID";
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
		UPDATE student_status set "status" = 'to_del' WHERE student_status."external_id" =
		old."ID";
        RETURN OLD;
    END IF;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.add_to_student_status()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    --mstr varchar(30);
    astr varchar(100);
    --retstr varchar(254);
BEGIN
    IF  TG_OP = 'INSERT'  or TG_OP = 'UPDATE' THEN
        astr = NEW."ID";
        --if		new."StudentStatusID" in ('8462edfe-55e3-4d64-b3d5-8933e9c82bed','d9d450b4-184e-4738-8710-25c53fed6263',
--'06365216-0c9b-4352-a80c-89a2bf8a3424', '450519a5-4a4a-477d-b1e4-4f7daf310065') then
		INSERT INTO student_status(id, external_id) values (nextval('serial'), astr);
		--end if;
        RETURN NEW;
    END IF;
   return new;
END;
$function$
;

create trigger check_insert_student after
insert
    on
    public.student for each row
    when (((new."StudentStatusID")::text = any (array['8462edfe-55e3-4d64-b3d5-8933e9c82bed'::text,
    'd9d450b4-184e-4738-8710-25c53fed6263'::text,
    '06365216-0c9b-4352-a80c-89a2bf8a3424'::text,
    '450519a5-4a4a-477d-b1e4-4f7daf310065'::text]))) execute procedure add_to_student_status();


create trigger check_delete_student after
delete
    on
    public.student for each row execute procedure update_student_status_from_student();

create trigger check_update_student_to_add after
update
    on
    public.student for each row
    when ((((new."StudentStatusID")::text = any (array['8462edfe-55e3-4d64-b3d5-8933e9c82bed'::text,
    'd9d450b4-184e-4738-8710-25c53fed6263'::text,
    '06365216-0c9b-4352-a80c-89a2bf8a3424'::text,
    '450519a5-4a4a-477d-b1e4-4f7daf310065'::text]))
        and ((old."StudentStatusID")::text <> all (array['8462edfe-55e3-4d64-b3d5-8933e9c82bed'::text,
        'd9d450b4-184e-4738-8710-25c53fed6263'::text,
        '06365216-0c9b-4352-a80c-89a2bf8a3424'::text,
        '450519a5-4a4a-477d-b1e4-4f7daf310065'::text])))) execute procedure add_to_student_status();

create trigger check_update_student_to_update after
update
    on
    public.student for each row
    when (((new."StudentStatusID")::text <> all (array['8462edfe-55e3-4d64-b3d5-8933e9c82bed'::text,
    'd9d450b4-184e-4738-8710-25c53fed6263'::text,
    '06365216-0c9b-4352-a80c-89a2bf8a3424'::text,
    '450519a5-4a4a-477d-b1e4-4f7daf310065'::text]))) execute procedure update_student_status_from_student();

CREATE OR REPLACE FUNCTION public.update_student_status_from_human()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    var varchar;
BEGIN
    IF TG_OP = 'UPDATE' then
    	var = (SELECT student."ID" from student where "HumanID" = new."ID");
		UPDATE student_status set "status" = 'new', "responce" = 'Обновлено в human'
		WHERE student_status."external_id" = var;
		--(SELECT student."ID" from student where "HumanID" = new."ID");
        RETURN NEW;
    END IF;
END;
$function$
;

create trigger check_update_human after
update
    on
    public.human for each row
    when ((((old."HumanFirstName")::text is distinct
from
    (new."HumanFirstName")::text)
        or ((old."HumanINN")::text is distinct
    from
        (new."HumanINN")::text)
            or ((old."HumanLastName")::text is distinct
        from
            (new."HumanLastName")::text)
                or ((old."HumanMiddleName")::text is distinct
            from
                (new."HumanMiddleName")::text)
                    or ((old."HumanSNILS")::text is distinct
                from
                    (new."HumanSNILS")::text))) execute procedure update_student_status_from_human()
_________________________________________________________________________________________________________________________

--Новая таблица по поступившим плюс триггер и функция для добавления в общую таблицу contingent_flow
CREATE TABLE public.enrollment_extract (
	external_id varchar NOT NULL,
	student varchar NULL,
	order_id varchar NULL,
	CONSTRAINT enrollment_extract_pkey PRIMARY KEY (external_id)
);

CREATE TABLE public.contingent_flows (
	external_id varchar NOT NULL,
	student varchar NOT NULL,
	contingent_flow varchar NULL,
	flow_type varchar NULL,
	"date" varchar NULL,
	faculty varchar NULL,
	education_form varchar NULL,
	form_fin varchar NULL,
	details varchar NULL,
	gisscos_id varchar NULL,
	status varchar NULL DEFAULT 'not_in_gis'::character varying,
	date_sync timestamp NULL DEFAULT now(),
	response varchar NULL,
	extract_type varchar NULL,
	studentstatusstr_p varchar(50) NULL,
	CONSTRAINT contingent_flows_pkey PRIMARY KEY (external_id)
);

-- Table Triggers

create trigger check_insert after
insert
    on
    public.enrollment_extract for each row execute procedure add_to_contingent();

--
CREATE OR REPLACE FUNCTION public.add_to_contingent()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    vstudent varchar(50);
    vexternal_id varchar(50);
BEGIN
    IF  TG_OP = 'INSERT' THEN
        vexternal_id = NEW."external_id";
        vstudent = NEW."student";
		INSERT INTO contingent_flows (external_id, student) values (vexternal_id, vstudent);
        RETURN NEW;
    END IF;
   return new;
END;
$function$
;

___________________________________________________________________________________________________________________

-- Создание таблиц
CREATE TABLE public.disciplines (
	external_id varchar(50) NOT NULL,
	title varchar NOT NULL,
	status varchar(10) NULL DEFAULT 'new'::character varying,
	date_sync timestamp NULL DEFAULT now(),
	responce varchar NULL,
	gisscos_id varchar NULL,
	CONSTRAINT discipline_pkey PRIMARY KEY (external_id)
);

alter table educational_programs rename to educational_programs_subject;
alter table educational_programs_subject drop column status;
alter table educational_programs_subject drop column responce;
alter table educational_programs_subject drop column date_sync;
alter table educational_programs_subject drop column gisscos_id;
alter table educational_programs_subject drop column start_year;
alter table educational_programs_subject drop column end_year;
CREATE TABLE public.educational_programs (
	external_id varchar NOT NULL,
	title varchar NULL,
	educational_program_id varchar NULL,
	status varchar NULL DEFAULT 'new'::character varying,
	responce varchar NULL,
	date_sync timestamp NULL DEFAULT now(),
	gisscos_id varchar NULL,
	CONSTRAINT educational_programs_version_pkey PRIMARY KEY (external_id)
);

CREATE TABLE public.study_plans (
	external_id varchar(50) NOT NULL,
	title varchar NOT NULL,
	direction varchar NULL,
	code_direction varchar NULL,
	start_year varchar NULL,
	end_year varchar NULL,
	education_form varchar NULL,
	educational_program varchar NULL,
	status varchar NULL DEFAULT 'new'::character varying,
	date_sync timestamp NULL DEFAULT now(),
	responce varchar NULL,
	direction_name varchar NULL,
	CONSTRAINT study_plans_pkey PRIMARY KEY (external_id)
);

CREATE TABLE public.subject (
	id varchar NOT NULL,
	direction varchar NULL,
	code_direction varchar NULL,
	CONSTRAINT subject_pkey PRIMARY KEY (id)
);

--Фукция, которая обновляет таблицу study_plans в соответствии с нужным форматом для отправки в ГИС, вызывается при
CREATE OR REPLACE FUNCTION public.update_study_plans()           -- срабатывании триггеров check_insert и check_update
	RETURNS trigger
	LANGUAGE plpgsql
AS $function$
declare
id varchar;
vperiod_year varchar;
vend_year varchar;
vdirection varchar;
vform varchar;
vform2 varchar;
	BEGIN
		if TG_OP = 'INSERT' or TG_OP = 'UPDATE' then
			id = new."external_id";
			vperiod_year = new."period_year";
			vform = new."education_form";
			if (select "code_direction" from study_plans where external_id = id ) is not null then
				if (select s.direction from subject s where s."code_direction" = new."code_direction") is not null then
					vdirection = (select s.direction from subject s where s."code_direction" = new."code_direction");
				else
					vdirection = new."direction_name";
				end if;
			else
				vdirection = new."direction_name";
			end if;

			if vform = 'Заочная'  or vform = 'EXTRAMURAL' then
				vform2 = 'EXTRAMURAL';
			elsif vform = 'Очная' or vform = 'FULL_TIME' then
				vform2 = 'FULL_TIME';
			elsif vform = 'Очно-заочная' or vform = 'PART_TIME' then
				vform2 = 'PART_TIME';
			else
				vform2 = 'error_type';
			end if;

		 	update study_plans set "start_year" = substring(vperiod_year from 1 for 4),
								   "end_year" = substring(vperiod_year from 8),
								   "direction" = vdirection,
								   "education_form" = vform2,
								   "responce" = 'Обновлено/добавлено из Тандема',
								   "status" = 'new',
								   "date_sync" = NOW()
			where external_id = id;
			if (select end_year from study_plans where external_id = id) = 'н.вр.' then
				update study_plans set "end_year" = to_char(current_date, 'YYYY')
				where external_id = id;
			end if;


			return new ;
		end if;
		return new;
	END;
$function$

-- Триггеры для таблицы study_plans, проверяющие добавление записи или обновление неоторых полей
create trigger check_insert after
insert
    on
    public.study_plans for each row execute procedure update_study_plans()


create trigger check_update after
update
    on
    public.study_plans for each row
    when ((((old.title)::text is distinct
from
    (new.title)::text)
        or ((old.direction_name)::text is distinct
    from
        (new.direction_name)::text)
            or ((old.period_year)::text is distinct
        from
            (new.period_year)::text)
                or ((old.education_form)::text is distinct
            from
                (new.education_form)::text)
                    or ((old.educational_program)::text is distinct
                from
                    (new.educational_program)::text))) execute procedure update_study_plans()


-- Функция, обновляющая таблицу educational_programs в соответствии с нужным форматом для отправки в ГИС, вызывается
CREATE OR REPLACE FUNCTION public.update_educational_programs()    -- при срабатывании триггеров check_insert
 RETURNS trigger                                                   -- и check_update таблицы educational_programs
 LANGUAGE plpgsql
AS $function$
declare
id varchar;
vstart_year varchar;
vend_year varchar;
	begin
		if TG_OP = 'INSERT' or TG_OP = 'UPDATE' then
			id = new."external_id";
			vstart_year = new."start_year";
			vend_year = new."end_year";
			update educational_programs set "direction" = (select direction from subject s where s.id = educational_programs.direction_id),
											"code_direction" = (select code_direction from subject s where s.id = educational_programs.direction_id)

			where "external_id" = id;
			return new;
		end if;
		return new;
	END;
$function$

--Триггеры для таблицы educational_programs, проверяющие добавление записи или обновление некоторых полей
create trigger check_insert after
insert
    on
    public.educational_programs for each row execute procedure update_educational_programs()


create trigger check_update after
update
    on
    public.educational_programs for each row
    when (((old.title)::text is distinct
from
    (new.title)::text)
        or ((old.direction_id)::text is distinct
    from
        (new.direction_id)::text)
    ) execute procedure update_educational_programs()

-- Функция, обновляющая таблицу disciplines в соответствии с нужным форматом для отправки в ГИС, вызывается
CREATE OR REPLACE FUNCTION public.update_disciplines()              -- при срабатывании триггеров check_insert
 RETURNS trigger                                                    -- и check_update таблицы disciplines
 LANGUAGE plpgsql
AS $function$
declare
id varchar;
	begin
		if TG_OP = 'UPDATE' then
			id = new."external_id";
			update disciplines set "status" = 'new',
								   "date_sync" = NOW(),
								   "responce" = null
			where "external_id" = id;
			return new;
		end if;
		return new;
	END;
$function$

-- Триггеры для таблицы disciplines, проверяющие добавление записи или обновление некоторых полей
create trigger check_update
after update
    on
    public.disciplines
    for each row
    when (((old.title)::text is distinct
from
    (new.title)::text))
    execute procedure update_disciplines()

CREATE TRIGGER check_insert
    AFTER INSERT
    ON public.disciplines
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_disciplines();