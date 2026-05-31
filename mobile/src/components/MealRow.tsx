import { StyleSheet, Text, View } from "react-native";

import type { GeneratedMeal } from "../types/api";

type MealRowProps = {
  meal: GeneratedMeal;
};

export function MealRow({ meal }: MealRowProps) {
  return (
    <View style={styles.row}>
      <View style={styles.main}>
        <Text style={styles.slot}>{String(meal.slot ?? "meal")}</Text>
        <Text style={styles.name}>{String(meal.display_name ?? meal.recipe_id ?? "Recipe")}</Text>
      </View>
      <View style={styles.macros}>
        <Text style={styles.macro}>{formatNumber(meal.kcal)} kcal</Text>
        <Text style={styles.macro}>{formatNumber(meal.protein_g)}g protein</Text>
      </View>
    </View>
  );
}

function formatNumber(value: unknown): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }
  return String(Math.round(value));
}

const styles = StyleSheet.create({
  row: {
    borderTopWidth: 1,
    borderTopColor: "#E5E0D5",
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 12,
    paddingTop: 10,
  },
  main: {
    flex: 1,
    gap: 2,
  },
  slot: {
    color: "#165D77",
    fontSize: 13,
    fontWeight: "800",
    textTransform: "capitalize",
  },
  name: {
    color: "#1F2933",
    fontSize: 15,
    fontWeight: "700",
  },
  macros: {
    alignItems: "flex-end",
    minWidth: 92,
  },
  macro: {
    color: "#4B5563",
    fontSize: 13,
    fontWeight: "700",
  },
});
