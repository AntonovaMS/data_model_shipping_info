-- ПЕРЕСМОТР МОДЕЛИ ДАННЫХ.МИГРАЦИЯ В ОТДЕЛЬНЫЕ ЛОГИЧЕСКИЕ ТАБЛИЦЫ

DROP TABLE IF EXISTS public.shipping_agreement CASCADE; 
DROP TABLE IF EXISTS public.shipping_transfer CASCADE; 
DROP TABLE IF EXISTS public.shipping_info CASCADE; 
DROP TABLE IF EXISTS public.shipping_status CASCADE; 
--shipping 

CREATE TABLE public.shipping( 
    ID serial , 
    shippingid                         BIGINT, 
    saleid                             BIGINT, 
    orderid                            BIGINT, 
    clientid                           BIGINT, 
    payment_amount                     NUMERIC(14,2), 
    state_datetime                    TIMESTAMP, 
    productid                          BIGINT, 
    description                       text, 
    vendorid                           BIGINT, 
    namecategory                      text, 
    base_country                      text, 
    status                            text, 
    state                             text, 
    shipping_plan_datetime            TIMESTAMP, 
    hours_to_plan_shipping           NUMERIC(14,2), 
    shipping_transfer_description     text, 
    shipping_transfer_rate           NUMERIC(14,3), 
    shipping_country                  text, 
    shipping_country_base_rate       NUMERIC(14,3), 
    vendor_agreement_description      text, 
    PRIMARY KEY (ID) 
); 
CREATE INDEX shippingid ON public.shipping (shippingid); 
COMMENT ON COLUMN public.shipping.shippingid is 'id of shipping of sale'; 

 

 

CREATE TABLE public.shipping_country_rates( 
    shipping_country_id     serial , 
    shipping_country        text , 
    shipping_country_base_rate  NUMERIC(14,3) , 
    PRIMARY KEY (shipping_country_id) 
); 
CREATE INDEX shipping_country_index ON public.shipping_country_rates(shipping_country); 

 

 

CREATE TABLE public.shipping_agreement( 
    agreementid     BIGINT , 
    agreement_number        text , 
    agreement_rate  NUMERIC(14,3) , 
    agreement_commission  NUMERIC(14,3), 
    PRIMARY KEY (agreementid) 
); 
CREATE INDEX agreement_id_index ON public.shipping_agreement(agreementid); 

 

 

CREATE TABLE public.shipping_transfer( 
    transfer_type_id     serial , 
    transfer_type        text , 
    transfer_model       text , 
    shipping_transfer_rate  NUMERIC(14,3), 
    PRIMARY KEY (transfer_type_id) 
); 
CREATE INDEX transfer_type_id_index ON public.shipping_transfer(transfer_type_id); 

 

CREATE TABLE public.shipping_info( 
    shippingid     BIGINT , 
    vendorid       BIGINT , 
    payment_amount       NUMERIC(14,2), 
    shipping_plan_datetime TIMESTAMP, 
    transfer_type_id    BIGINT   , 
    shipping_country_id  BIGINT   , 
    agreementid  BIGINT  , 
    PRIMARY KEY (shippingid), 
    FOREIGN KEY (transfer_type_id) REFERENCES public.shipping_transfer(transfer_type_id) ON UPDATE cascade, 
    FOREIGN KEY (shipping_country_id) REFERENCES public.shipping_country_rates(shipping_country_id) ON UPDATE cascade, 
    FOREIGN KEY (agreementid) REFERENCES public.shipping_agreement(agreementid) ON UPDATE cascade 
); 
CREATE INDEX shippingid_info_index ON public.shipping_info(shippingid); 

 

CREATE TABLE public.shipping_status( 
    shippingid     BIGINT , 
    status        text , 
    state         text , 
    shipping_start_fact_datetime  TIMESTAMP, 
    shipping_end_fact_datetime  TIMESTAMP, 
    PRIMARY KEY (shippingid) 
); 
CREATE INDEX shipping_status_index ON public.shipping_status(status); 

 

INSERT INTO public.shipping_country_rates  (shipping_country, shipping_country_base_rate) 
select T1.* 
FROM(SELECT distinct shipping_country  ,shipping_country_base_rate from public.shipping s)T1; 


INSERT INTO public.shipping_agreement (agreementid ,agreement_number ,agreement_rate ,agreement_commission) 
SELECT distinct CAST((regexp_split_to_array(vendor_agreement_description, E'\\:+'))[1] as BIGINT) as agreementid, 
    (regexp_split_to_array(vendor_agreement_description, E'\\:+'))[2] as agreementid_number, 
    cast((regexp_split_to_array(vendor_agreement_description, E'\\:+'))[3] as NUMERIC(14,3)) as agreement_rate, 
    CAST((regexp_split_to_array(vendor_agreement_description, E'\\:+'))[4]  as NUMERIC(14,3)) as  agreement_commission 
FROM PUBLIC.SHIPPING; 

 
INSERT INTO public.shipping_transfer (transfer_type ,transfer_model ,shipping_transfer_rate) 
SELECT T1.* 
FROM (SELECT DISTINCT(regexp_split_to_array(shipping_transfer_description, E'\\:+'))[1] AS transfer_type , 
                     (regexp_split_to_array(shipping_transfer_description, E'\\:+'))[2] AS transfer_model, 
                     shipping_transfer_rate FROM PUBLIC.shipping)T1; 


INSERT INTO public.shipping_info (shippingid ,vendorid ,payment_amount ,shipping_plan_datetime ,transfer_type_id ,shipping_country_id ,agreementid) 
select distinct s.shippingid, 
                s.vendorid,  
                s.payment_amount, 
                s.shipping_plan_datetime,  
                t1.transfer_type_id  , 
                t2.shipping_country_id,  
                CAST((regexp_split_to_array(s.vendor_agreement_description, E'\\:+'))[1] as BIGINT) AS agreementid 
from public.shipping s 
LEFT join (select * from public.shipping_transfer)t1 on s.shipping_transfer_description = t1.transfer_type||':'||t1.transfer_model 
LEFT join (select * from public.shipping_country_rates)t2 on s.shipping_country = t2.shipping_country; 

 
insert into public.shipping_status (shippingid ,status ,state ,shipping_start_fact_datetime ,shipping_end_fact_datetime) 
select t2.shippingid, 
        t2.status, 
        t2.state, 
        t3.ms as shipping_start_fact_datetime, 
        t4.mas as shipping_end_fact_datetime 
from(select t1.shippingid, t1.status, t1.state 
    from(select shippingid , s.state_datetime ,s.status, s.state, rank() OVER (PARTITION BY shippingid ORDER BY s.state_datetime DESC) as r 
    from public.shipping s) t1 where t1.r = 1) t2 
LEFT join  (select shippingid, state_datetime as ms from public.shipping where state = 'booked') t3 on t2.shippingid= t3.shippingid 
LEFT join  (select shippingid, state_datetime as mas from public.shipping where state = 'recieved')t4 on t2.shippingid = t4.shippingid; 

 

 --ПОСТРОЕНИЕ ВИТРИНЫ

CREATE OR REPLACE VIEW shipping_datamart AS  
select si.shippingid,  
        si.vendorid,  
        st.transfer_type, 
        extract( day from (shipping_end_fact_datetime-shipping_start_fact_datetime)) as full_day_at_shipping, 
        case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 1 else 0 end is_delay, 
        case when ss.status = 'finished' then 1 else 0 end is_shipping_finish, 
        case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then extract(day from (shipping_end_fact_datetime - shipping_plan_datetime))  end delay_day_at_shipping, 
        si.payment_amount, 
        si.payment_amount*(scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) as vat, 
        si.payment_amount * sa.agreement_commission as profit 
from public.shipping_info si 
LEFT join public.shipping_transfer st on si.transfer_type_id = st.transfer_type_id 
LEFT join public.shipping_status ss on ss.shippingid = si.shippingid 
LEFT join public.shipping_country_rates scr on scr.shipping_country_id = si.shipping_country_id 
LEFT join public.shipping_agreement sa on sa.agreementid = si.agreementid;