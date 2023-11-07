
# for dt in `seq 20220609 1 20220625` 
# do
#     bee --hivevar dt=${dt} -f 1_mobile_pay_shop_daily.sql
#     bee --hivevar dt=${dt} -f 2_mobile_payment_transport_session_daily.sql
#     bee --hivevar dt=${dt} -f 3_mobile_payment_session_daily.sql
#     bee --hivevar dt=${dt} -f 4_mobile_payment_mapping_daily.sql
# done

# dt='20230220'
# bee --hivevar dt=${dt} -f 1_mobile_pay_host_daily.sql
# bee --hivevar dt=${dt} -f 2_mobile_payment_transport_session_daily.sql
# bee --hivevar dt=${dt} -f 3_mobile_payment_session_daily.sql
# bee --hivevar dt=${dt} -f 4_mobile_payment_mapping_daily.sql

# for dt in `seq 20230301 1 20230305` 
# do
#     # bee --hivevar dt=${dt} -f 1_mobile_pay_host_daily.sql
#     # bee --hivevar dt=${dt} -f 1_mobile_pay_shop_daily.sql
#     bee --hivevar dt=${dt} -f 2_mobile_payment_transport_session_daily.sql
#     bee --hivevar dt=${dt} -f 3_mobile_payment_session_daily.sql
#     # bee --hivevar dt=${dt} -f 4_mobile_payment_mapping_daily.sql
# done

# for dt in `seq 20230310 1 20230313` 
# do
#     bee --hivevar dt=${dt} -f 1_mobile_pay_host_daily.sql
#     # bee --hivevar dt=${dt} -f 1_mobile_pay_shop_daily.sql
#     bee --hivevar dt=${dt} -f 2_mobile_payment_transport_session_daily.sql
#     bee --hivevar dt=${dt} -f 3_mobile_payment_session_daily.sql
#     # bee --hivevar dt=${dt} -f 4_mobile_payment_mapping_daily.sql
# done

# dt=20230322 
# bee --hivevar dt=${dt} -f 1_mobile_pay_host_daily.sql
# # bee --hivevar dt=${dt} -f 1_mobile_pay_shop_daily.sql
# bee --hivevar dt=${dt} -f 2_mobile_payment_transport_session_daily.sql
# bee --hivevar dt=${dt} -f 3_mobile_payment_session_daily.sql
# # bee --hivevar dt=${dt} -f 4_mobile_payment_mapping_daily.sql


# for dt in `seq 20230406 1 20230422` 
# do
#     # bee --hivevar dt=${dt} -f 1_mobile_pay_host_daily.sql
#     # bee --hivevar dt=${dt} -f 1_mobile_pay_shop_daily.sql
#     # bee --hivevar dt=${dt} -f 2_mobile_payment_transport_session_daily.sql
#     bee --hivevar dt=${dt} -f 3_mobile_payment_session_daily.sql
#     bee --hivevar dt=${dt} -f 4_mobile_payment_mapping_daily.sql
# done


# for dt in `seq 20230424 1 20230426` 
# do
#     # bee --hivevar dt=${dt} -f 1_mobile_pay_host_daily.sql
#     # bee --hivevar dt=${dt} -f 1_mobile_pay_shop_daily.sql
#     # bee --hivevar dt=${dt} -f 2_mobile_payment_transport_session_daily.sql
#     bee --hivevar dt=${dt} -f 3_mobile_payment_session_daily.sql
#     bee --hivevar dt=${dt} -f 4_mobile_payment_mapping_daily.sql
# done

# dt='20230831'

# bee --hivevar dt=${dt} -f 1_mobile_pay_host_daily.sql

for dt in `seq 20230903 1 20230914` 
do
    bee --hivevar dt=${dt} -f 1_mobile_pay_host_daily.sql
done
