import type { ReactNode } from "react";
import { StyleSheet, Text, View } from "react-native";

type StatusKind = "idle" | "loading" | "connected" | "error";

type StatusCardProps = {
  title: string;
  status: StatusKind;
  children: ReactNode;
};

const STATUS_LABELS: Record<StatusKind, string> = {
  idle: "Not checked",
  loading: "Checking",
  connected: "Connected",
  error: "Error",
};

const STATUS_COLORS: Record<StatusKind, string> = {
  idle: "#6B7280",
  loading: "#2563EB",
  connected: "#147A4A",
  error: "#B42318",
};

export function StatusCard({ title, status, children }: StatusCardProps) {
  return (
    <View style={styles.card}>
      <View style={styles.headerRow}>
        <Text style={styles.title}>{title}</Text>
        <Text style={[styles.badge, { color: STATUS_COLORS[status] }]}>
          {STATUS_LABELS[status]}
        </Text>
      </View>
      <View style={styles.content}>{children}</View>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    borderWidth: 1,
    borderColor: "#D9D6CC",
    borderRadius: 8,
    backgroundColor: "#FFFFFF",
    padding: 16,
  },
  headerRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 12,
  },
  title: {
    color: "#1F2933",
    fontSize: 16,
    fontWeight: "700",
  },
  badge: {
    fontSize: 13,
    fontWeight: "700",
  },
  content: {
    marginTop: 14,
    gap: 8,
  },
});
