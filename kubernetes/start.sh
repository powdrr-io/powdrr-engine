make engine
minikube start --memory 12000 --cpus 8 --disk-size 50g --driver docker
eval $(minikube -p minikube docker-env)
minikube image load <image_name>
kubectl apply -f deployment.yaml
