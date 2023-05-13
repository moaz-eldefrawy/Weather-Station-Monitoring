# Installation

This is a documentation of the steps I followed to run a kubernetes cluster locally on my windows machine

1. Install kubectl: [link](https://kubernetes.io/docs/tasks/tools/install-kubectl-windows/#install-kubectl-binary-with-curl-on-windows)
2. Install kind: [link](https://kind.sigs.k8s.io/docs/user/quick-start/#installation). I downloaded the release binary from [here](https://github.com/kubernetes-sigs/kind/releases), renamed it to kind.exe, and add it to the environment variables.

# Run

3. Create a cluster with: `kind create cluster --config kubernetes\kind-config.yaml`. This create a cluster named "kind". 
4. Set kubectl context to the created cluster `kubectl config use-context kind-kind`
5. Start zookeeper replicas/service: `kubectl apply -f kubernetes\zookeeper.yaml`. (Note, these replicas should be allowed to run on the control-plane node)
6. Verify everthing is working: `kubectl get pods -o wide` (after some time, you should see three replicas on all three different nodes)
7. Start kafka: `kubectl apply -f kubernetes\kafka.yaml`


### Helpful Commands
Stop a "service": `kubectl delete -f kubernetes\kafka.yaml`
## Deviations from project spec

ThThe project pdf asks for "2 services with Kafka and Zookeeper images", but Zookeeper requires a minimum of three nodes. So we have three replicas, and they are allowed to run on the control plane node.