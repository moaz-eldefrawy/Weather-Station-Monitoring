# Installation

This is a documentation of the steps I followed to run a kubernetes cluster locally on my windows machine

1. Install kubectl: [link](https://kubernetes.io/docs/tasks/tools/install-kubectl-windows/#install-kubectl-binary-with-curl-on-windows)
2. Install kind: [link](https://kind.sigs.k8s.io/docs/user/quick-start/#installation). I downloaded the release binary from [here](https://github.com/kubernetes-sigs/kind/releases), renamed it to kind.exe, and add it to the environment variables.
3. Pull the needed images locally: `docker pull bitnami/kafka:3.4.0`
# Run

1. Create a cluster with: `kind create cluster --config kubernetes\kind-config.yaml`. This create a cluster named "kind"
2. Load docker images that will be used by the cluster: `kind load docker-image weather-station-mock:1.0.0 central-station:1.0.0 bitnami/kafka:3.4.0`
3. Set kubectl context to the created cluster `kubectl config use-context kind-kind`
4. Start kafka: `kubectl apply -f kubernetes\kafka.yaml`
5. (Optional) Verify everthing is working: `kubectl get pods -o wide`
6. Start the weather-station-mock: `kubectl apply -f kubernetes\weather-station-mock.yaml`
7. Check that the weather station is producing records: `kubectl exec --stdin --tty kafka -- /bin/bash`, then `kafka-console-consumer.sh --topic weather-station-status --from-beginning --bootstrap-server kafka:9092` and wait a few moments
8. Start the central-station: `kubectl apply -f kubernetes\central-station.yaml`

### Helpful Commands
Delete the cluster: `kind delete cluster -n kind`\
Stop a "service": `kubectl delete -f kubernetes\kafka.yaml`\
Shell into a service: `kubectl exec --stdin --tty kafka -- /bin/bash`
