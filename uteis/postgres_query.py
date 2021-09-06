
class PostgresQuery:
    def __init__(self):
        self.city = """select 
                id, 
                now() at time zone 'BRT' as  bigquery_ingestion 
            from city"""
        self.business = """select 
                id,
                description,
                user_id,
                created at time zone 'BRT' as created,
                flg_validado,
                now() at time zone 'BRT' as  bigquery_ingestion
            from business where created >= '{}'"""
        self.user_account = """select 
                id,
                email,
                now() at time zone 'BRT' as  bigquery_ingestion
            from user_account 
            where created >= '{}' or 
                updated_at >= '{}'"""
        self.state = """select
                id,
                name,
                initials,
                now() at time zone 'BRT' as  bigquery_ingestion
            from state"""
        self.business_status = """select
                id,
                slug,
                now() at time zone 'BRT' as bigquery_ingestion
            from business_status"""
        self.business_task = """select
                id,
                task_id,
                user_id,
                now() at time zone 'BRT' as bigquery_ingestion
            from business_task where created >= '{}' or 
                updated_at >= '{}'"""
        self.task_quote = """select
                id,
                task_id,
                user_id,
                amount,
                updated_at AT TIME ZONE 'BRT' as updated_at,
                now() at time zone 'BRT' as bigquery_ingestion 
            from task_quote where created >= '{}' or 
                updated_at >= '{}'"""
        self.service = """select
                id,
                text,
                img,
                now() at time zone 'BRT' as bigquery_ingestion 
            from service"""
        self.task_problem = """select
                id,
                task_id,
                updated_at AT TIME ZONE 'BRT' as updated_at,
                now() at time zone 'BRT' as bigquery_ingestion 
            from task_problem where created >= '{}' or 
                updated_at >= '{}'"""
        self.company = """select
                id,
                name,
                logo_url,
                is_active,
                city_id,
                state_id,
                commission_deal_date AT TIME ZONE 'BRT' as commission_deal_date,
                now() at time zone 'BRT' as bigquery_ingestion 
            from company"""
        self.contact = """select
                id,
                slug,
                created AT TIME ZONE 'BRT' as created,
                now() at time zone 'BRT' as bigquery_ingestion 
            from contact"""
        self.task = """select
                id,
                description,
                address,
                phone,
                now() at time zone 'BRT' as bigquery_ingestion from contact
            from task where created >= '{}' or 
                updated_at >= '{}'"""
        self.responsys_contact_list = """select  
                ua.id,
                ua.email,
                upper(split_part(ua.name,' ', 1)) as first_name,
                upper((case when array_length(regexp_split_to_array(ua.name, '\s+'), 1) = 1 then 
                    null else split_part(ua.name,' ', array_length(regexp_split_to_array(ua.name, '\s+'), 1)) end
                )) as last_name,
                ua.phone,
                s.name as state,
                c.name as city,
                ua.birthday,
                ua.source,
                ua.gender,
                (case when b.id is null then 'B2C' else 'PRO' end) as user_type
            from user_account ua
            left join business b ON b.user_id = ua.id 
            left join city c on ua.city_id = c.id 
            left join state s on ua.state_id = s.id
            where 
                ua.phone is not null or ua.email is not null
            group by ua.id,
                ua.email,
                upper(split_part(ua.name,' ', 1)),
                upper((case when array_length(regexp_split_to_array(ua.name, '\s+'), 1) = 1 then 
                    null else split_part(ua.name,' ', array_length(regexp_split_to_array(ua.name, '\s+'), 1)) end
                )),
                ua.phone,
                s.name,
                c.name,
                ua.birthday,
                ua.source,
                ua.gender,
                (case when b.id is null then 'B2C' else 'PRO' end)
            order by ua.id"""
        self.responsys_status_slug = """select 
                user_id, 
                status_slug 
            from business where id in (
	            select max(id) from business group by user_id)"""





