import { StyleSheet, Text, View } from "react-native";

import { PrimaryButton } from "./AppButton";
import { AppCard } from "./AppCard";

type EmptyStateProps = {
  actionLabel?: string;
  onAction?: () => void;
  text: string;
  title: string;
};

export function EmptyState({ actionLabel, onAction, text, title }: EmptyStateProps) {
  return (
    <View style={styles.wrapper}>
      <AppCard>
        <View style={styles.content}>
          <Text style={styles.title}>{title}</Text>
          <Text style={styles.text}>{text}</Text>
        </View>
        {actionLabel && onAction ? (
          <PrimaryButton label={actionLabel} onPress={onAction} />
        ) : null}
      </AppCard>
    </View>
  );
}

const styles = StyleSheet.create({
  content: {
    gap: 6,
  },
  text: {
    color: "#4B5563",
    fontSize: 15,
    lineHeight: 21,
  },
  title: {
    color: "#111827",
    fontSize: 20,
    fontWeight: "800",
  },
  wrapper: {
    flex: 1,
    justifyContent: "center",
    minHeight: 420,
  },
});
