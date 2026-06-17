import { StyleSheet, Text, View } from "react-native";

const BADGE_SIZE = 34;

export function VideoPlayBadge() {
  return (
    <View pointerEvents="none" style={styles.badge}>
      <Text style={styles.icon}>▶</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  badge: {
    alignItems: "center",
    backgroundColor: "rgba(255,255,255,0.82)",
    borderRadius: BADGE_SIZE / 2,
    height: BADGE_SIZE,
    justifyContent: "center",
    position: "absolute",
    width: BADGE_SIZE,
  },
  icon: {
    color: "#74B72E",
    fontSize: 14,
    fontWeight: "900",
    marginLeft: 2,
  },
});
