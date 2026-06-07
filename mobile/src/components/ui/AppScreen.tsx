import type { ReactNode } from "react";
import { ScrollView, StatusBar, StyleSheet } from "react-native";

import { colors } from "../../theme/colors";

type AppScreenProps = {
  children: ReactNode;
};

export function AppScreen({ children }: AppScreenProps) {
  return <ScrollView contentContainerStyle={styles.container}>{children}</ScrollView>;
}

const TOP_SAFE_PADDING = Math.max(StatusBar.currentHeight ?? 0, 24) + 16;

const styles = StyleSheet.create({
  container: {
    backgroundColor: colors.background,
    flexGrow: 1,
    gap: 18,
    paddingBottom: 148,
    paddingHorizontal: 18,
    paddingTop: TOP_SAFE_PADDING,
  },
});
