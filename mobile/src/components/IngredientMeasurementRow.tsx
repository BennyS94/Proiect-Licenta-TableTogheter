import { StyleSheet, Text, View } from "react-native";

import { colors } from "../theme/colors";
import {
  getIngredientMeasurementIconKind,
  IngredientMeasurementIcon,
} from "./icons/ingredient/IngredientMeasurementIcons";

type IngredientMeasurementRowProps = {
  text: string;
};

export function IngredientMeasurementRow({ text }: IngredientMeasurementRowProps) {
  const iconKind = getIngredientMeasurementIconKind({ rawText: text });

  return (
    <View style={styles.row}>
      <View style={styles.iconCircle}>
        <IngredientMeasurementIcon color={colors.accentDark} kind={iconKind} size={20} />
      </View>
      <Text style={styles.text}>{text}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  iconCircle: {
    alignItems: "center",
    backgroundColor: "#EEF7E8",
    borderColor: "#DDEAD3",
    borderRadius: 15,
    borderWidth: 1,
    height: 30,
    justifyContent: "center",
    width: 30,
  },
  row: {
    alignItems: "center",
    flexDirection: "row",
    gap: 9,
  },
  text: {
    color: colors.muted,
    flex: 1,
    fontSize: 13,
    lineHeight: 18,
  },
});
