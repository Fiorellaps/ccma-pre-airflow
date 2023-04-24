import yaml



with open("gfk_vgfk_csv.yaml") as file:
        list_doc = yaml.safe_load(file)
        print(list_doc["spec"]["sparkConf"]["spark.hadoop.fs.s3a.access.key"])
        endpoint = "http://rook-ceph-rgw-my-store-rook-ceph.apps.k8spro.nextret.net:8080"
        endpoint = endpoint.split("://")[1]
        print(endpoint[1])
        list_doc["spec"]["sparkConf"]["spark.hadoop.fs.s3a.access.key"] = "newKey"
        print(list_doc["spec"]["sparkConf"]["spark.hadoop.fs.s3a.access.key"])
