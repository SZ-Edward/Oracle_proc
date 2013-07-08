create or replace procedure FIREWALL_POLY_APPROVAL_RATE(
	o_return_code   out varchar2,
	o_return_msg    out varchar2,
	i_month         in varchar2,
	i_province_code in char
) is

  --临时变量
  v_month_tmp     varchar2(8) := '';            --考核月份
  v_score_rule    number(5, 2) := 0.00;         --扣分规则
  v_score_limited number(5, 2) := 0.00;         --扣分上限
  v_score_actual  number(5, 2) := 0.00;         --实际扣分
  v_score_aft_crt number(5, 2) := 0.00;         --修正后的分数
  v_para_id       varchar2(20) := '2013060702'; --参数ID，kpi_para
  v_rule_id       varchar2(20) := '20021';      --规则ID，kpi_rule
  v_rule_type     varchar2(2) := '02';          --归属大类，kpi_rule
  v_rule_class    varchar2(2) := '21';          --规则分类，kpi_rule
  v_counter       number := 0;                  --不符合考核条件的工单order_id记录的计数器
  v_province_code char(3);                      --省代码
  
  type cursor_file is ref cursor; --定义游标变量类型
  v_cursor_file cursor_file;      --声明游标变量   

  --自定义异常,用来跳出当前循环,继续下一次循环,类似continue的效果
  continue_exception EXCEPTION;
  PRAGMA EXCEPTION_INIT(continue_exception, -1401);
  
BEGIN
  /*
  ********************************参数校验 begin********************************
  */
  o_return_code := '0';
  o_return_msg  := '考核成功';

  --判断考核月份是否为空，为空则结束存储过程 
  if i_month is null then
    o_return_code := '-1';
    o_return_msg  := '考核月份不能为空。';
    return;
  end if;

  --校验考核月份的格式，不符合格式则结束存储过程
  begin
    select to_char(to_date(i_month, 'YYYYMM'), 'YYYYMM')
      into v_month_tmp
      from dual;
  Exception
    when others then
      o_return_code := '-1';
      o_return_msg  := '考核月份的格式出错，格式为:YYYYMM。 ' || SQLERRM;
      return;
  end;

  --校验考核省份的格式，不符合格式则结束存储过程
  begin    
    if i_province_code is not null then
      select to_number(i_province_code) into v_province_code from dual;
      if length(i_province_code) != 3 then
        o_return_code := '-1';
        o_return_msg  := '考核省份的格式出错，格式为三位数字.。 ' || SQLERRM;
        return;
      end if;
    end if;
    Exception
      when others then
        o_return_code := '-1';
        o_return_msg  := '考核省份的格式出错，格式为三位数字.。 ' || SQLERRM;
        return;
  end;

  --查询防火墙策略审批率扣分规则 ，判断是否为空，为空则结束存储过程
  begin
    select r.score
      into v_score_rule
      from kpi_rule r
     where r.rule_id = v_rule_id
       and r.rule_type = v_rule_type
       and r.rule_class = v_rule_class;
  Exception
    when no_data_found then
      o_return_code := '-1';
      o_return_msg  := '防火墙策略审批率扣分规则为空 。 ';
      return;
  end;

  --查询防火墙策略审批率上限，判断是否为空，为空则结束存储过程
  begin
    select to_number(p.para_value, '99.99')
      into v_score_limited
      from kpi_para p
     where p.para_id = v_para_id
       and p.rule_type = v_rule_type;
  Exception
    when no_data_found then
      o_return_code := '-1';
      o_return_msg  := '防火墙策略审批率扣分上限为空 。 ';
      return;
  end;
  /*
  ********************************参数校验 end********************************
  */

  /*
  ********************************逻辑处理 begin********************************
  */
  --保证考核不重复（删除相同考核条件的数据）
  if i_province_code is null then
    delete from kpi_check_rec rcd
     where rcd.rule_id = v_rule_id
       and rcd.rule_type = v_rule_type
       and rcd.rule_class = v_rule_class
       and rcd.check_cycle = v_month_tmp;
  else
    delete from kpi_check_rec rcd
     where rcd.rule_id = v_rule_id
       and rcd.rule_type = v_rule_type
       and rcd.rule_class = v_rule_class
       and rcd.check_cycle = v_month_tmp
       and rcd.province = i_province_code;
  end if;

  /**
  对INT_FIREWALL_ACL_ORDER_DY表中order_id字段为空和
  INT_FIREWALL_ACL_ORDER_DY表中order_id字段值在INT_FIREWALL_ACL_ORDER_MO表中不存在的记录
  进行计数
  */
  begin
    if i_province_code is null then
      open v_cursor_file for
        select a.province, count(1)
          from (select distinct d.province, d.res_name, d.ip, to_char(d.begin_time, 'yyyymmdd') as bt
                  from INT_FIREWALL_ACL_ORDER_DY d, INT_FIREWALL_ACL_ORDER_MO m
                 where d.province = m.province
                   and d.rpt_month = m.rpt_month
                   and (d.order_id is null or d.order_id != m.order_id)
                   and to_char(d.begin_time, 'yyyymm') = v_month_tmp
                 group by d.res_name, d.ip, d.order_id, d.province, to_char(d.begin_time, 'yyyymmdd')) a
         group by a.province;
    else
      open v_cursor_file for     
        select a.province, count(1)
          from (select distinct d.province, d.res_name, d.ip, to_char(d.begin_time, 'yyyymmdd') as bt
                  from INT_FIREWALL_ACL_ORDER_DY d, INT_FIREWALL_ACL_ORDER_MO m
                 where d.province = m.province
                   and d.rpt_month = m.rpt_month
                   and (d.order_id is null or d.order_id != m.order_id)
                   and to_char(d.begin_time, 'yyyymm') = v_month_tmp
                   and d.province = v_province_code
                 group by d.res_name, d.ip, d.order_id, d.province, to_char(d.begin_time, 'yyyymmdd')) a
         group by a.province;
    end if;
    Exception
      when no_data_found then return;
  end;
  
  --循环游标  
  loop
    v_province_code :=  null;
      fetch v_cursor_file into v_province_code, v_counter;
      exit when v_cursor_file%notfound; --没有数据则结束循环
      
    begin
      if v_province_code is null then
        --v_province_code变量值为空则跳出当前循环,继续下一趟循环
        raise continue_exception;
      end if;
    
      if v_counter is null then
        --v_counter变量值为空则跳出当前循环,继续下一趟循环
        raise continue_exception;
      elsif v_counter >= 1 then
        v_score_actual := v_counter * v_score_rule;
      end if;
    Exception when continue_exception then null;
    end;    
    
    if v_score_actual > v_score_limited then
      v_score_aft_crt := v_score_limited;
    else 
      v_score_aft_crt := v_score_actual;
    end if;

  if v_province_code is not null then
    --将考核结果存入kpi_check_rec表
    insert into kpi_check_rec
      (province,
       check_cycle,
       rule_id,
       rule_type,
       score,
       score_after_corrected,
       remark,
       check_time,
       rule_class)
    values
      (v_province_code,
       v_month_tmp,
       v_rule_id,
       v_rule_type,
       v_score_actual,
       v_score_aft_crt,
       v_month_tmp || '月份的防火墙策略审批率考核数据',
       sysdate,
       v_rule_class);
   end if;
  
  end loop;
  close v_cursor_file;
  /*
  ********************************逻辑处理 end********************************
  */
Exception
  when others then
    o_return_code := '-1';
    o_return_msg  := sqlerrm;
    
END FIREWALL_POLY_APPROVAL_RATE;
