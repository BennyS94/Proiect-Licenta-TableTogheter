import { ActivityIndicator, Pressable, StyleSheet, Text } from "react-native";

type AppButtonProps = {
  disabled?: boolean;
  label: string;
  loading?: boolean;
  onPress: () => void;
  variant?: "primary" | "secondary";
};

export function AppButton({
  disabled,
  label,
  loading,
  onPress,
  variant = "primary",
}: AppButtonProps) {
  const secondary = variant === "secondary";
  return (
    <Pressable
      accessibilityRole="button"
      disabled={disabled}
      onPress={onPress}
      style={({ pressed }) => [
        styles.button,
        secondary ? styles.secondary : null,
        pressed && !disabled ? styles.pressed : null,
        disabled ? styles.disabled : null,
      ]}
    >
      {loading ? (
        <ActivityIndicator color={secondary ? "#165D77" : "#FFFFFF"} />
      ) : (
        <Text style={[styles.label, secondary ? styles.secondaryLabel : null]}>{label}</Text>
      )}
    </Pressable>
  );
}

export function PrimaryButton(props: Omit<AppButtonProps, "variant">) {
  return <AppButton {...props} variant="primary" />;
}

export function SecondaryButton(props: Omit<AppButtonProps, "variant">) {
  return <AppButton {...props} variant="secondary" />;
}

const styles = StyleSheet.create({
  button: {
    alignItems: "center",
    backgroundColor: "#165D77",
    borderRadius: 8,
    justifyContent: "center",
    minHeight: 46,
    paddingHorizontal: 14,
  },
  disabled: {
    opacity: 0.55,
  },
  label: {
    color: "#FFFFFF",
    fontSize: 15,
    fontWeight: "800",
    textAlign: "center",
  },
  pressed: {
    opacity: 0.82,
  },
  secondary: {
    backgroundColor: "#FFFFFF",
    borderColor: "#165D77",
    borderWidth: 1,
  },
  secondaryLabel: {
    color: "#165D77",
  },
});
