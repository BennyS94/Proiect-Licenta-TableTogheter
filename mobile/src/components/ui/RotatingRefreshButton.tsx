import { useCallback, useRef, useState } from "react";
import { Animated, Easing, Pressable, StyleProp, StyleSheet, ViewStyle } from "react-native";

import { RefreshArrowsIcon } from "../icons/RefreshArrowsIcon";

type RotatingRefreshButtonProps = {
  accessibilityLabel?: string;
  color?: string;
  disabled?: boolean;
  durationMs?: number;
  iconSize?: number;
  onPress: () => void;
  size?: number;
  style?: StyleProp<ViewStyle>;
};

export function RotatingRefreshButton({
  accessibilityLabel = "Refresh",
  color,
  disabled = false,
  durationMs = 720,
  iconSize = 22,
  onPress,
  size = 36,
  style,
}: RotatingRefreshButtonProps) {
  const rotation = useRef(new Animated.Value(0)).current;
  const isRotatingRef = useRef(false);
  const [isRotating, setIsRotating] = useState(false);

  const handlePress = useCallback(() => {
    if (disabled || isRotatingRef.current) {
      return;
    }

    onPress();
    isRotatingRef.current = true;
    setIsRotating(true);
    rotation.setValue(0);
    Animated.timing(rotation, {
      duration: durationMs,
      easing: Easing.out(Easing.cubic),
      toValue: 1,
      useNativeDriver: true,
    }).start(() => {
      rotation.setValue(0);
      isRotatingRef.current = false;
      setIsRotating(false);
    });
  }, [disabled, durationMs, onPress, rotation]);

  const spin = rotation.interpolate({
    inputRange: [0, 1],
    outputRange: ["0deg", "360deg"],
  });

  return (
    <Pressable
      accessibilityLabel={accessibilityLabel}
      accessibilityRole="button"
      disabled={disabled || isRotating}
      onPress={handlePress}
      style={({ pressed }) => [
        styles.button,
        {
          borderRadius: size / 2,
          height: size,
          width: size,
        },
        style,
        pressed && !disabled ? styles.pressed : null,
        disabled ? styles.disabled : null,
      ]}
    >
      <Animated.View style={{ transform: [{ rotate: spin }] }}>
        <RefreshArrowsIcon color={color} size={iconSize} />
      </Animated.View>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  button: {
    alignItems: "center",
    backgroundColor: "rgba(255,255,255,0.72)",
    borderColor: "rgba(116,183,46,0.22)",
    borderWidth: 1,
    justifyContent: "center",
  },
  disabled: {
    opacity: 0.48,
  },
  pressed: {
    opacity: 0.82,
  },
});
