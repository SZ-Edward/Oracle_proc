create or replace procedure ETL_TREASURY_MODE_MGR_COLLECT(
  o_return_code    out varchar2,
  o_return_msg     out varchar2,
  i_month              in varchar2,
  i_province_code in char
) is

  --临时变量
  v_province_code     char(3);           --省代码
  v_month_tmp         varchar2(8) := ''; --汇总月份
  v_context_id        number(2);         --场景
  v_num_of_request    number(10);        --请求总数
  v_num_of_permit     number(10);        --允许总数
  v_num_of_refuse     number(10);        --拒绝总数
  v_num_of_timeout    number(10);        --超时总数

  type cursor_treasury is ref cursor;
  v_cursor_treasury cursor_treasury;

BEGIN
/*
********************************参数校验 begin********************************
*/
  o_return_code := '0';
  o_return_msg  := '汇总成功';

  --判断汇总月份是否为空，为空则结束存储过程 
  if i_month is null then
    o_return_code := '-1';
    o_return_msg  := '汇总月份不能为空。';
    return;
  end if;

  --校验汇总月份的格式，不符合格式则结束存储过程
  begin
    select to_char(to_date(i_month, 'YYYYMM'), 'YYYYMM')
      into v_month_tmp
      from dual;
  Exception
    when others then
      o_return_code := '-1';
      o_return_msg  := '汇总月份的格式出错，格式为:YYYYMM。 ' || SQLERRM;
      return;
  end;
  
  --校验汇总省份的格式，不符合格式则结束存储过程
  begin    
    if i_province_code is not null then
      select to_number(i_province_code) into v_province_code from dual;
      if length(i_province_code) != 2 then
        o_return_code := '-1';
        o_return_msg  := '汇总省份的格式出错，格式为三位数字.。 ' || SQLERRM;
        return;
      end if;
    end if;
    Exception
      when others then
        o_return_code := '-1';
        o_return_msg  := '汇总省份的格式出错，格式为三位数字.。 ' || SQLERRM;
        return;
  end;
/*
********************************参数校验 end********************************
*/

/*
********************************逻辑处理 begin********************************
*/ 
if i_province_code is null then  
   delete from rpt_4a_auth_case_sum r where r.rpt_cycle = v_month_tmp;
   open v_cursor_treasury for 
      select t.p, t.m, t.c, nvl(t.v, 0) sq, nvl(t1.v, 0) cg, nvl(t2.v, 0) jj, nvl(t3.v, 0) cs
        from (select ay.province p, to_char(ay.begin_time, 'yyyymmdd') m, ay.context_id c, ay.request_id r, count(1) v
                from int_4a_apply_operation_dy ay
               group by ay.province, to_char(ay.begin_time, 'yyyymmdd'), ay.context_id, ay.request_id) t
        left outer join (select ap.province p, to_char(ap.begin_time, 'yyyymmdd') m, ap.request_id r, count(1) v
                           from int_4a_approve_operation_dy ap
                          where ap.result = '0'
                          group by ap.province, to_char(ap.begin_time, 'yyyymmdd'), ap.request_id) t1
          on (t.p = t1.p and t.m = t1.m and t.r = t1.r)
        left outer join (select ap.province p, to_char(ap.begin_time, 'yyyymmdd') m, ap.request_id r, count(1) v
                           from int_4a_approve_operation_dy ap
                          where ap.result = '1'
                          group by ap.province, to_char(ap.begin_time, 'yyyymmdd'), ap.request_id) t2
          on (t2.p = t1.p and t2.m = t1.m and t2.r = t1.r)
        left outer join (select ap.province p, to_char(ap.begin_time, 'yyyymmdd') m, ap.request_id r, count(1) v
                           from int_4a_approve_operation_dy ap
                          where ap.result = '2'
                          group by ap.province,  to_char(ap.begin_time, 'yyyymmdd'), ap.request_id) t3
          on (t2.p = t3.p and t2.m = t3.m and t2.r = t3.r)
       where t.m = to_char(v_month_tmp, 'yyyymmdd');
else
   delete from rpt_4a_auth_case_sum r where r.rpt_cycle = v_month_tmp and r.province = i_province_code;
   open v_cursor_treasury for 
       select t.p, t.m, t.c, nvl(t.v, 0) sq, nvl(t1.v, 0) cg, nvl(t2.v, 0) jj, nvl(t3.v, 0) cs
        from (select ay.province p, to_char(ay.begin_time, 'yyyymmdd') m, ay.context_id c, ay.request_id r, count(1) v
                from int_4a_apply_operation_dy ay
               group by ay.province, to_char(ay.begin_time, 'yyyymmdd'), ay.context_id, ay.request_id) t
        left outer join (select ap.province p, to_char(ap.begin_time, 'yyyymmdd') m, ap.request_id r, count(1) v
                           from int_4a_approve_operation_dy ap
                          where ap.result = '0'
                          group by ap.province, to_char(ap.begin_time, 'yyyymmdd'), ap.request_id) t1
          on (t.p = t1.p and t.m = t1.m and t.r = t1.r)
        left outer join (select ap.province p, to_char(ap.begin_time, 'yyyymmdd') m, ap.request_id r, count(1) v
                           from int_4a_approve_operation_dy ap
                          where ap.result = '1'
                          group by ap.province, to_char(ap.begin_time, 'yyyymmdd'), ap.request_id) t2
          on (t2.p = t1.p and t2.m = t1.m and t2.r = t1.r)
        left outer join (select ap.province p, to_char(ap.begin_time, 'yyyymmdd') m, ap.request_id r, count(1) v
                           from int_4a_approve_operation_dy ap
                          where ap.result = '2'
                          group by ap.province,  to_char(ap.begin_time, 'yyyymmdd'), ap.request_id) t3
          on (t2.p = t3.p and t2.m = t3.m and t2.r = t3.r)
       where t.m = to_char(v_month_tmp, 'yyyymmdd')
         and t.p = i_province_code;
end if;

loop
  fetch v_cursor_treasury 
    into v_province_code, v_month_tmp, v_context_id, v_num_of_request, v_num_of_permit, v_num_of_refuse, v_num_of_timeout; 
  
  exit when v_cursor_treasury%notfound; --没有数据则结束循环
  
  insert into rpt_4a_auth_case_sum(
    province, rpt_cycle, context_id, qqvalue, yyvalue, jjvalue, csvalue, rpt_time
  ) values(
    v_province_code, 
    v_month_tmp, 
    v_context_id, 
    v_num_of_request,   
    v_num_of_permit, 
    v_num_of_refuse, 
    v_num_of_timeout, 
    sysdate
  );
    
end loop;

close v_cursor_treasury;
/*
********************************逻辑处理 end********************************
*/  
Exception
  when others then
    o_return_code := '-1';
    o_return_msg  := sqlerrm;
      
END ETL_TREASURY_MODE_MGR_COLLECT;
