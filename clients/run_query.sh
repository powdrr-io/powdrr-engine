max=1000
for i in `seq 2 $max`
do
    curl --request POST -H "Content-Type: application/json" --data @query.json http://localhost:9200/test_index1/_search
done

