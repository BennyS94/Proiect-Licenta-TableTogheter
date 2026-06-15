import type { ComponentType } from "react";
import { StyleSheet, Text, View } from "react-native";

import {
  CaloriesFlameIcon,
  CarbsWheatIcon,
  FatsAvocadoIcon,
  ProteinDrumstickIcon,
} from "../icons/MacroNutrientIcons";
import { colors } from "../../theme/colors";

type MacroKind = "calories" | "protein" | "carbs" | "fat";

type MacroMiniStatProps = {
  iconSize?: number;
  kind: MacroKind;
  tone?: "strong" | "soft";
  value: string;
};

const ICONS: Record<MacroKind, ComponentType<{ color?: string; size?: number }>> = {
  calories: CaloriesFlameIcon,
  carbs: CarbsWheatIcon,
  fat: FatsAvocadoIcon,
  protein: ProteinDrumstickIcon,
};

const SOFT_ICON_COLORS: Record<MacroKind, string> = {
  calories: "#374151",
  carbs: "#C98A3A",
  fat: "#C6A63A",
  protein: "#C96A70",
};

export function MacroMiniStat({
  iconSize = 18,
  kind,
  tone = "strong",
  value,
}: MacroMiniStatProps) {
  const Icon = ICONS[kind];
  const color = tone === "soft" ? SOFT_ICON_COLORS[kind] : undefined;

  return (
    <View style={styles.container}>
      <Icon color={color} size={iconSize} />
      <Text numberOfLines={1} style={styles.value}>
        {value}
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    alignItems: "center",
    flexDirection: "row",
    gap: 4,
    minWidth: 0,
  },
  value: {
    color: colors.text,
    flexShrink: 1,
    fontSize: 13,
    fontWeight: "900",
  },
});
