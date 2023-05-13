# Installation

This is a documentation of the steps I followed to run a kubernetes cluster locally on my windows machine

1. Install kubectl: [link](https://kubernetes.io/docs/tasks/tools/install-kubectl-windows/#install-kubectl-binary-with-curl-on-windows)
2. Install kind: [link](https://kind.sigs.k8s.io/docs/user/quick-start/#installation). I downloaded the release binary from [here](https://github.com/kubernetes-sigs/kind/releases), renamed it to kind.exe, and add it to the environment variables.

# Run

3. Create a cluster with: `kind create cluster --config kubernetes\kind-config.yaml`. This create a cluster named "kind". 
4. Set kubectl context to the created cluster `kubectl config use-context kind-kind`
5. Start kafka: `kubectl apply -f kubernetes\kafka.yaml`.
6. Verify everthing is working: `kubectl get pods -o wide`, `kubectl exec --stdin --tty kafka -- /bin/bash`

### Helpful Commands
Stop a "service": `kubectl delete -f kubernetes\kafka.yaml`
