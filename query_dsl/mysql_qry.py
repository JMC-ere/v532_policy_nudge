# -*- coding: utf-8 -*-

query = {
    "PRD": {
        "SELECT": """
select 
   ndata.*
   ,expo.menu_id 
   ,expo.floating_yn
   ,nudge.nudge_type
   ,sugg.`type`
   ,sm.model stb_model
   ,expo.btn_type ui_type
   ,date_format(adddate(now(), 0), '%Y%m%d%H%i%s') nudge_date
   ,date_format(adddate(now(), {day}), '%Y.%m.%d') log_time
   ,'v532' stb_ver
   , case 
      when ndata.exposure_id  = '2'
         then  (select concat(esa.try_cnt,',' ,time_cnt) from nudge_common.exposure_slot_attr esa where esa.exposure_id  = '2' and nudge_id ='2') 
      else 
         null
   end zapping_policy
   , case 
      when ndata.exposure_id  = '2'
         then  (select esa.show_time from nudge_common.exposure_slot_attr esa where esa.exposure_id  = '2' and nudge_id ='2')
      else 
         null
   end display_epg_policy
   , case 
      when ndata.exposure_id  = '1'
         then  (select concat(esa.show_time,',', esa.roll_time) from  nudge_common.exposure_slot_attr esa where esa.exposure_id  = '1' and esa.nudge_id ='1') 
      else 
         null
   end display_breaktime_policy
   , case 
      when ndata.exposure_id  = '1'
         then (select esa.show_time from  nudge_common.exposure_slot_attr esa where esa.exposure_id  = '1' and esa.nudge_id ='14')
      else 
         null
   end display_specific_policy
   , case 
      when ndata.exposure_id  not in ('1', '2') 
         then  (select concat(esa.show_time,',', esa.roll_time) from  nudge_common.exposure_slot_attr esa where esa.exposure_id  = ndata.exposure_id  ) 
      else null
    end display_general_policy
from (
   select 
      s.slot_id
      ,se.seg_id 
      ,concat('seg_',se.seg_id, '_', seg.seg_name) seg
      ,ssm.stb_type       stb_group
      ,s.slot_idx
      ,s.start_dt
      ,s.end_dt
      ,s.all_stb_mdl_yn
      ,s.exposure_id
      ,s.nudge_id
      ,s.suggest_id
      ,s.icon_id          img_type
      ,s.set_slot_cnt
      ,s.text_id
       ,case 
         when s.text_id is not null 
            then Json_Array(
               concat(
                  '<span style="color:'
                  , (select value from nudge_common.system_setting ss  where name = 'base_color')
                  , '\">' 
                  , nt.front_text
                  , '</span>'
                  ,'<span style="color:'
                  , (select value from nudge_common.system_setting ss  where name = 'acc_color')
                  , '"><b>'
               ),
               concat(
                  '</b></span>'
                  ,'<span style="color:'
                  , (select value from nudge_common.system_setting ss  where name = 'base_color')
                  , '\">' 
                  , nt.back_text
                  , '</span>'
               )
            )
         else 
            replace(
         		replace(s.manual_text, 'ㅴacc_colorㅴ', (select value from nudge_common.system_setting ss  where name = 'acc_color'))
         		, 'ㅴbase_colorㅴ'
         		, (select value from nudge_common.system_setting ss  where name = 'base_color')
         	)
      end txt  
      ,IFNULL(s.ext_info, '[]') ext_info
      ,s.once_yn specific_once_yn
      , case when s.nudge_id  = '14' then s.start_dt
         else null 
      end specific_time
      ,s.service_id specific_service_id
      ,concat('/nudge/',s.file_saved_name) img_url
   from nudge_v532.slot s
   inner join nudge_v532.slot_seg se on s.slot_id  = se.slot_id
   inner join nudge_v532.seg seg on se.seg_id  = seg.seg_id and seg.del_yn ='N' and seg.use_yn  = 'Y'
   left outer join nudge_v532.slot_stb_model ssm on s.slot_id = ssm.slot_id
   left outer join nudge_common.nudge_text nt on s.text_id  = nt.text_id 
   where  1=1
		and (
			date_format(adddate(now(), {day}), '%Y%m%d000000') between s.start_dt and ifnull(s.end_dt, s.start_dt)
			or (
				-- 특정채널 넛지 처리
				s.nudge_id = '14'
				and	date_format(adddate(now(), {day}), '%Y%m%d') =  DATE_FORMAT(STR_TO_DATE(s.start_dt, '%Y%m%d%H%i%s'), '%Y%m%d')
			)
		)
) ndata
left outer join nudge_common.exposure expo on ndata.exposure_id = expo.exposure_id
left outer join nudge_common.nudge nudge on ndata.nudge_id = nudge.nudge_id
left outer join nudge_common.suggest sugg on ndata.suggest_id = sugg.suggest_id
left outer join nudge_common.stb_model sm on ndata.stb_group = sm.stb_type
        """,
        "INSERT_SLOT_HIST": """
        insert into nudge_v532.slot_hist (
        save_date
        ,slot_id
        ,slot_idx
        ,start_dt
        ,end_dt
        ,exposure_id
        ,nudge_id
        ,suggest_id
        ,icon_id
        ,set_slot_cnt
        ,service_id
        ,text_id
        ,manual_text
        ,ext_info
        ,once_yn
        ,all_stb_mdl_yn
        ,apply_user_id
        ,apply_dt
        ,file_name
        ,file_saved_name
        )
        select
        date_format(adddate(now(), {day}), '%Y%m%d')
        ,slot_id
        ,slot_idx
        ,start_dt
        ,end_dt
        ,exposure_id
        ,s.nudge_id
        ,suggest_id
        ,icon_id
        ,set_slot_cnt
        ,service_id
        ,s.text_id
        ,manual_text
        ,ext_info
        ,once_yn
        ,all_stb_mdl_yn
        ,s.apply_user_id
        ,s.apply_dt
        ,file_name
        ,file_saved_name
        from nudge_v532.slot  s
        where 
             -- date_format(adddate(now(), {day}), '%Y%m%d%H%i%s') between s.start_dt and ifnull(s.end_dt, s.start_dt)
            date_format(adddate(now(), {day}), '%Y%m%d000000') between s.start_dt and ifnull(s.end_dt, s.start_dt)
			or (
				-- 특정채널 넛지 처리
				s.nudge_id = '14'
				and	date_format(adddate(now(), {day}), '%Y%m%d') =  DATE_FORMAT(STR_TO_DATE(s.start_dt, '%Y%m%d%H%i%s'), '%Y%m%d')
			)
        """,
        "INSERT_SLOT_SEG_HIST": """
        insert into nudge_v532.slot_seg_hist (
        save_date
        ,slot_id
        ,seg_id
        )
        select
        date_format(adddate(now(), {day}), '%Y%m%d')
        ,slot_id
        ,seg_id 
        from nudge_v532.slot_seg ss
        where 
        ss.slot_id  in (
        select
         slot_id
        from nudge_v532.slot s
        where 
           --date_format(adddate(now(), {day}), '%Y%m%d%H%i%s') between s.start_dt and ifnull(s.end_dt, s.start_dt)
           date_format(adddate(now(), {day}), '%Y%m%d000000') between s.start_dt and ifnull(s.end_dt, s.start_dt)
			or (
				-- 특정채널 넛지 처리
				s.nudge_id = '14'
				and	date_format(adddate(now(), {day}), '%Y%m%d') =  DATE_FORMAT(STR_TO_DATE(s.start_dt, '%Y%m%d%H%i%s'), '%Y%m%d')
			)
        )
        """,
        "INSERT_SLOT_MODEL_HIST": """
        insert into nudge_v532.slot_stb_model_hist (
        save_date
        ,slot_id
        ,stb_type
        )

        select
        date_format(adddate(now(), {day}), '%Y%m%d')
        ,slot_id
        ,stb_type 
        from nudge_v532.slot_stb_model sm
        where 
        sm.slot_id  in (
        select
         slot_id
        from nudge_v532.slot s
        where 
          -- date_format(adddate(now(), {day}), '%Y%m%d%H%i%s') between s.start_dt and ifnull(s.end_dt, s.start_dt)
          date_format(adddate(now(), {day}), '%Y%m%d000000') between s.start_dt and ifnull(s.end_dt, s.start_dt)
			or (
				-- 특정채널 넛지 처리
				s.nudge_id = '14'
				and	date_format(adddate(now(), {day}), '%Y%m%d') =  DATE_FORMAT(STR_TO_DATE(s.start_dt, '%Y%m%d%H%i%s'), '%Y%m%d')
			)
        )
        """,
        "DELETE_SLOT_HIST": """
        delete from nudge_v532.slot_hist where save_date=date_format(adddate(now(), {day}), '%Y%m%d')
        """,
        "DELETE_SLOT_SEG_HIST": """
        delete from nudge_v532.slot_seg_hist where save_date=date_format(adddate(now(), {day}), '%Y%m%d')
        """,
        "DELETE_SLOT_MODEL_HIST": """
        delete from nudge_v532.slot_stb_model_hist where save_date=date_format(adddate(now(), {day}), '%Y%m%d')
        """,
        "DELETE_ES_DSL": {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"stb_ver": "v532"}}
                    ]
                }
            }
        }
    },
    "STG": {
        "SELECT": """
select 
   ndata.*
   ,expo.menu_id 
   ,expo.floating_yn
   ,nudge.nudge_type
   ,sugg.`type`
   ,sm.model stb_model
   ,expo.btn_type ui_type
   ,date_format(adddate(now(), {day}), '%Y%m%d%H%i%s') nudge_date
   ,date_format(adddate(now(), {day}), '%Y.%m.%d') log_time
   ,'v532' stb_ver
   , case 
      when ndata.exposure_id  = '2'
         then  (select concat(esa.try_cnt,',' ,time_cnt) from nudge_common.exposure_slot_attr esa where esa.exposure_id  = '2' and nudge_id ='2') 
      else 
         null
   end zapping_policy
   , case 
      when ndata.exposure_id  = '2'
         then  (select esa.show_time from nudge_common.exposure_slot_attr esa where esa.exposure_id  = '2' and nudge_id ='2')
      else 
         null
   end display_epg_policy
   , case 
      when ndata.exposure_id  = '1'
         then  (select concat(esa.show_time,',', esa.roll_time) from  nudge_common.exposure_slot_attr esa where esa.exposure_id  = '1' and esa.nudge_id ='1') 
      else 
         null
   end display_breaktime_policy
   , case 
      when ndata.exposure_id  = '1'
         then (select esa.show_time from  nudge_common.exposure_slot_attr esa where esa.exposure_id  = '1' and esa.nudge_id ='14')
      else 
         null
   end display_specific_policy
   , case 
      when ndata.exposure_id  not in ('1', '2') 
         then  (select concat(esa.show_time,',', esa.roll_time) from  nudge_common.exposure_slot_attr esa where esa.exposure_id  = ndata.exposure_id  ) 
      else null
    end display_general_policy
from (
   select 
      s.slot_id
      ,se.seg_id 
      ,concat('seg_',se.seg_id, '_', seg.seg_name) seg
      ,ssm.stb_type       stb_group
      ,s.slot_idx
      ,s.start_dt
      ,s.end_dt
      ,s.all_stb_mdl_yn
      ,s.exposure_id
      ,s.nudge_id
      ,s.suggest_id
      ,s.icon_id          img_type
      ,s.set_slot_cnt
      ,s.text_id
       ,case 
         when s.text_id is not null 
            then Json_Array(
               concat(
                  '<span style="color:'
                  , (select value from nudge_common.system_setting ss  where name = 'base_color')
                  , '\">' 
                  , nt.front_text
                  , '</span>'
                  ,'<span style="color:'
                  , (select value from nudge_common.system_setting ss  where name = 'acc_color')
                  , '"><b>'
               ),
               concat(
                  '</b></span>'
                  ,'<span style="color:'
                  , (select value from nudge_common.system_setting ss  where name = 'base_color')
                  , '\">' 
                  , nt.back_text
                  , '</span>'
               )
            )
         else 
            replace(
         		replace(s.manual_text, 'ㅴacc_colorㅴ', (select value from nudge_common.system_setting ss  where name = 'acc_color'))
         		, 'ㅴbase_colorㅴ'
         		, (select value from nudge_common.system_setting ss  where name = 'base_color')
         	)
      end txt  
      ,IFNULL(s.ext_info, '[]') ext_info
      ,s.once_yn specific_once_yn
      , case when s.nudge_id  = '14' then s.start_dt
         else null 
      end specific_time
      ,s.service_id specific_service_id
      ,concat('/nudge/',s.file_saved_name) img_url
   from nudge_v532.slot s
   inner join nudge_v532.slot_seg se on s.slot_id  = se.slot_id
   inner join nudge_v532.seg seg on se.seg_id  = seg.seg_id and seg.del_yn ='N' and seg.use_yn  = 'Y'
   left outer join nudge_v532.slot_stb_model ssm on s.slot_id = ssm.slot_id
   left outer join nudge_common.nudge_text nt on s.text_id  = nt.text_id 
   where  1=1
		and (
			date_format(adddate(now(), {day}), '%Y%m%d000000') between s.start_dt and ifnull(s.end_dt, s.start_dt)
			or (
				-- 특정채널 넛지 처리
				s.nudge_id = '14'
				and	date_format(adddate(now(), {day}), '%Y%m%d') =  DATE_FORMAT(STR_TO_DATE(s.start_dt, '%Y%m%d%H%i%s'), '%Y%m%d')
			)
		)
) ndata
left outer join nudge_common.exposure expo on ndata.exposure_id = expo.exposure_id
left outer join nudge_common.nudge nudge on ndata.nudge_id = nudge.nudge_id
left outer join nudge_common.suggest sugg on ndata.suggest_id = sugg.suggest_id
left outer join nudge_common.stb_model sm on ndata.stb_group = sm.stb_type
        """,
        "INSERT_SLOT_HIST": """
        insert into nudge_v532.slot_hist (
        save_date
        ,slot_id
        ,slot_idx
        ,start_dt
        ,end_dt
        ,exposure_id
        ,nudge_id
        ,suggest_id
        ,icon_id
        ,set_slot_cnt
        ,service_id
        ,text_id
        ,manual_text
        ,ext_info
        ,once_yn
        ,all_stb_mdl_yn
        ,apply_user_id
        ,apply_dt
        ,file_name
        ,file_saved_name
        )
        select
        date_format(adddate(now(), {day}), '%Y%m%d')
        ,slot_id
        ,slot_idx
        ,start_dt
        ,end_dt
        ,exposure_id
        ,s.nudge_id
        ,suggest_id
        ,icon_id
        ,set_slot_cnt
        ,service_id
        ,s.text_id
        ,manual_text
        ,ext_info
        ,once_yn
        ,all_stb_mdl_yn
        ,s.apply_user_id
        ,s.apply_dt
        ,file_name
        ,file_saved_name
        from nudge_v532.slot  s
        where 
			date_format(adddate(now(), {day}), '%Y%m%d000000') between s.start_dt and ifnull(s.end_dt, s.start_dt)
			or (
				-- 특정채널 넛지 처리
				s.nudge_id = '14'
				and	date_format(adddate(now(), {day}), '%Y%m%d') =  DATE_FORMAT(STR_TO_DATE(s.start_dt, '%Y%m%d%H%i%s'), '%Y%m%d')
			)
        """,
        "INSERT_SLOT_SEG_HIST": """
        insert into nudge_v532.slot_seg_hist (
        save_date
        ,slot_id
        ,seg_id
        )
        select
        date_format(adddate(now(), {day}), '%Y%m%d')
        ,slot_id
        ,seg_id 
        from nudge_v532.slot_seg ss
        where 
        ss.slot_id  in (
        select
         slot_id
        from nudge_v532.slot s
        where 
           -- date_format(adddate(now(), {day}), '%Y%m%d%H%i%s') between s.start_dt and ifnull(s.end_dt, s.start_dt)
            date_format(adddate(now(), {day}), '%Y%m%d000000') between s.start_dt and ifnull(s.end_dt, s.start_dt)
			or (
				-- 특정채널 넛지 처리
				s.nudge_id = '14'
				and	date_format(adddate(now(), {day}), '%Y%m%d') =  DATE_FORMAT(STR_TO_DATE(s.start_dt, '%Y%m%d%H%i%s'), '%Y%m%d')
			)           
        )
        """,
        "INSERT_SLOT_MODEL_HIST": """
        insert into nudge_v532.slot_stb_model_hist (
        save_date
        ,slot_id
        ,stb_type
        )
        select
        date_format(adddate(now(), {day}), '%Y%m%d')
        ,slot_id
        ,stb_type 
        from nudge_v532.slot_stb_model sm
        where 
        sm.slot_id  in (
        select
         slot_id
        from nudge_v532.slot s
        where 
          -- date_format(adddate(now(), {day}), '%Y%m%d%H%i%s') between s.start_dt and ifnull(s.end_dt, s.start_dt)
            date_format(adddate(now(), {day}), '%Y%m%d000000') between s.start_dt and ifnull(s.end_dt, s.start_dt) 
			or (
				-- 특정채널 넛지 처리
				s.nudge_id = '14'
				and	date_format(adddate(now(), {day}), '%Y%m%d') =  DATE_FORMAT(STR_TO_DATE(s.start_dt, '%Y%m%d%H%i%s'), '%Y%m%d')
			)     
        )
        """,
        "DELETE_SLOT_HIST": """
        delete from nudge_v532.slot_hist where save_date=date_format(adddate(now(), {day}), '%Y%m%d')
        """,
        "DELETE_SLOT_SEG_HIST": """
        delete from nudge_v532.slot_seg_hist where save_date=date_format(adddate(now(), {day}), '%Y%m%d')
        """,
        "DELETE_SLOT_MODEL_HIST": """
        delete from nudge_v532.slot_stb_model_hist where save_date=date_format(adddate(now(), {day}), '%Y%m%d')
        """,
        "DELETE_ES_DSL": {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"stb_ver": "v532"}}
                    ]
                }
            }
        }
    }
}
