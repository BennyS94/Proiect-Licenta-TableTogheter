import { StyleSheet, Text, View } from "react-native";

import { colors } from "../../theme/colors";
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
    alignItems: "center",
    gap: 6,
  },
  text: {
    color: colors.muted,
    fontSize: 15,
    lineHeight: 21,
    textAlign: "center",
  },
  title: {
    color: colors.text,
    fontSize: 20,
    fontWeight: "800",
    textAlign: "center",
  },
  wrapper: {
    flex: 1,
    justifyContent: "center",
    minHeight: 420,
  },
});
