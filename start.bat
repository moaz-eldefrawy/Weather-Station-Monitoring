kind delete cluster
kind create cluster --config kubernetes\kind-config.yaml
kind load docker-image weather-station-mock:1.0.0 central-station:1.0.0 bitnami/kafka:3.4.0 docker.elastic.co/elasticsearch/elasticsearch:8.7.0 docker.elastic.co/kibana/kibana:8.7.0 loader:1.0.0
kubectl config use-context kind-kind