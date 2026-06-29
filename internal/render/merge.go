package render

import corev1 "k8s.io/api/core/v1"

func mergeMap(base map[string]string, overlay map[string]string) map[string]string {
	out := cloneMap(base)
	if out == nil {
		out = map[string]string{}
	}
	for key, value := range overlay {
		out[key] = value
	}
	return out
}

func mergeContainers(existing []corev1.Container, required corev1.Container) []corev1.Container {
	out := []corev1.Container{required}
	for _, container := range existing {
		if container.Name == required.Name {
			out[0] = mergeContainer(container, required)
			continue
		}
		out = append(out, *container.DeepCopy())
	}
	return out
}

func mergeContainer(base corev1.Container, required corev1.Container) corev1.Container {
	out := *base.DeepCopy()
	out.Name = required.Name
	if required.Image != "" {
		out.Image = required.Image
	}
	if required.ImagePullPolicy != "" {
		out.ImagePullPolicy = required.ImagePullPolicy
	}
	out.Ports = append([]corev1.ContainerPort(nil), required.Ports...)
	out.ReadinessProbe = required.ReadinessProbe.DeepCopy()
	out.LivenessProbe = required.LivenessProbe.DeepCopy()
	if len(required.Resources.Limits) > 0 || len(required.Resources.Requests) > 0 {
		out.Resources = *required.Resources.DeepCopy()
	}
	out.VolumeMounts = mergeVolumeMounts(base.VolumeMounts, required.VolumeMounts)
	return out
}

func mergeVolumeMounts(existing []corev1.VolumeMount, required []corev1.VolumeMount) []corev1.VolumeMount {
	out := make([]corev1.VolumeMount, 0, len(required)+len(existing))
	seen := map[string]bool{}
	for _, mount := range required {
		out = append(out, mount)
		seen[mount.Name] = true
	}
	for _, mount := range existing {
		if seen[mount.Name] {
			continue
		}
		out = append(out, mount)
	}
	return out
}

func mergeVolumes(existing []corev1.Volume, required []corev1.Volume) []corev1.Volume {
	out := make([]corev1.Volume, 0, len(required)+len(existing))
	seen := map[string]bool{}
	for _, volume := range required {
		out = append(out, volume)
		seen[volume.Name] = true
	}
	for _, volume := range existing {
		if seen[volume.Name] {
			continue
		}
		out = append(out, *volume.DeepCopy())
	}
	return out
}
