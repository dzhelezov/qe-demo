# Distributed Query Engine Demo

To run, first generate the test data
```sh
python generate_test_data.py
```

Next, run the two nodes (`node1` and `node2`) and the demo server
```sh
make run
```

The demo page will be available at `localhost:8000`. The nodes are listening at ports 4999 and 5001 respecively.


To stop, run
```sh
make kill
```
