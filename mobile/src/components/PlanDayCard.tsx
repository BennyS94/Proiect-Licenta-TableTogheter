import { StyleSheet, Text, View } from "react-native";

import type { GeneratedDay } from "../types/api";
import { MealRow } from "./MealRow";

type PlanDayCardProps = {
  day: GeneratedDay;
};

export function PlanDayCard({ day }: PlanDayCardProps) {
  const meals = Array.isArray(day.selected_meals) ? day.selected_meals : [];

  return (
    <View style={styles.card}>
      <View style={styles.headerRow}>
        <Text style={styles.title}>Day {day.day_index ?? 1}</Text>
        <Text style={styles.totals}>
          {formatNumber(day.totals?.kcal)} kcal · {formatNumber(day.totals?.protein_g)}g protein
        </Text>
      </View>
      <View style={styles.meals}>
        {meals.map((meal, index) => (
          <MealRow key={`${meal.slot ?? "meal"}-${meal.recipe_id ?? index}`} meal={meal} />
        ))}
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
  card: {
    borderWidth: 1,
    borderColor: "#D9D6CC",
    borderRadius: 8,
    backgroundColor: "#FFFFFF",
    padding: 16,
    gap: 12,
  },
  headerRow: {
    gap: 4,
  },
  title: {
    color: "#111827",
    fontSize: 17,
    fontWeight: "800",
  },
  totals: {
    color: "#4B5563",
    fontSize: 14,
    fontWeight: "700",
  },
  meals: {
    gap: 10,
  },
});
