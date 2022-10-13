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

CREATE TABLE public.educational_programs (
	external_id varchar NOT NULL,
	title varchar NOT NULL,
	direction varchar NULL,
	code_direction varchar NULL,
	start_year varchar NULL,
	end_year varchar NULL,
	direction_id varchar NOT NULL,
	status varchar NULL DEFAULT 'new'::character varying,
	responce varchar NULL,
	date_sync timestamp NULL DEFAULT now(),
	gisscos_id varchar NULL,
	CONSTRAINT eduprogram_pkey PRIMARY KEY (external_id)
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
											"code_direction" = (select code_direction from subject s where s.id = educational_programs.direction_id),
								  			"start_year" = substring(vstart_year from 1 for 4),
								  			"end_year" = substring(vend_year from 1 for 4),
											"status" = 'new',
											"date_sync" = NOW(),
											"responce" = 'Обновлено/добавлено из Тандема'
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
    when ((((old.title)::text is distinct
from
    (new.title)::text)
        or ((old.direction_id)::text is distinct
    from
        (new.direction_id)::text)
            or ((old.start_year)::text is distinct
        from
            (new.start_year)::text)
                or ((old.end_year)::text is distinct
            from
                (new.end_year)::text))) execute procedure update_educational_programs()

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