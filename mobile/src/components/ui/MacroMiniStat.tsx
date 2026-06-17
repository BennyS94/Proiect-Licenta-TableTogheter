import type { ComponentType } from "react";
import { StyleSheet, Text, View } from "react-native";
import type { TextStyle } from "react-native";

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
  valueColor?: string;
  valueSize?: number;
  valueWeight?: TextStyle["fontWeight"];
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
  valueColor = colors.text,
  valueSize = 13,
  valueWeight = "900",
}: MacroMiniStatProps) {
  const Icon = ICONS[kind];
  const color = tone === "soft" ? SOFT_ICON_COLORS[kind] : undefined;

  return (
    <View style={styles.container}>
      <Icon color={color} size={iconSize} />
      <Text
        numberOfLines={1}
        style={[
          styles.value,
          {
            color: valueColor,
            fontSize: valueSize,
            fontWeight: valueWeight,
          },
        ]}
      >
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
    flexShrink: 1,
  },
});
