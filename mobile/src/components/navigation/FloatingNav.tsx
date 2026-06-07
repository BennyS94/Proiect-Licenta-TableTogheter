import { Pressable, StyleSheet, Text, View } from "react-native";

export type AppPageKey = "home" | "mealPlan" | "insights" | "household";

type FloatingNavProps = {
  activePage: AppPageKey;
  onSelectPage: (page: AppPageKey) => void;
};

const ITEMS: Array<{
  icon: string;
  key: AppPageKey;
  label: string;
}> = [
  { icon: "H", key: "home", label: "Home" },
  { icon: "M", key: "mealPlan", label: "Meal Plan" },
  { icon: "I", key: "insights", label: "Insights" },
  { icon: "A", key: "household", label: "Household and Account" },
];

export function FloatingNav({ activePage, onSelectPage }: FloatingNavProps) {
  return (
    <View style={styles.shell} pointerEvents="box-none">
      <View style={styles.bar}>
        {ITEMS.map((item) => {
          const active = item.key === activePage;
          return (
            <Pressable
              accessibilityLabel={item.label}
              accessibilityRole="button"
              key={item.key}
              onPress={() => onSelectPage(item.key)}
              style={({ pressed }) => [
                styles.item,
                active ? styles.itemActive : null,
                pressed ? styles.pressed : null,
              ]}
            >
              <Text style={[styles.icon, active ? styles.iconActive : null]}>{item.icon}</Text>
            </Pressable>
          );
        })}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  bar: {
    alignItems: "center",
    backgroundColor: "#FFFFFF",
    borderColor: "#D9D6CC",
    borderRadius: 26,
    borderWidth: 1,
    flexDirection: "row",
    gap: 8,
    justifyContent: "space-between",
    padding: 8,
    shadowColor: "#000000",
    shadowOffset: { width: 0, height: 4 },
    shadowOpacity: 0.16,
    shadowRadius: 16,
  },
  icon: {
    color: "#4B5563",
    fontSize: 17,
    fontWeight: "900",
  },
  iconActive: {
    color: "#FFFFFF",
  },
  item: {
    alignItems: "center",
    borderRadius: 20,
    height: 42,
    justifyContent: "center",
    width: 54,
  },
  itemActive: {
    backgroundColor: "#165D77",
  },
  pressed: {
    opacity: 0.82,
  },
  shell: {
    bottom: 18,
    left: 18,
    position: "absolute",
    right: 18,
  },
});
