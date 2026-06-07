import type { ReactNode } from "react";
import { ScrollView, StatusBar, StyleSheet } from "react-native";

type AppScreenProps = {
  children: ReactNode;
};

export function AppScreen({ children }: AppScreenProps) {
  return <ScrollView contentContainerStyle={styles.container}>{children}</ScrollView>;
}

const TOP_SAFE_PADDING = Math.max(StatusBar.currentHeight ?? 0, 24) + 16;

const styles = StyleSheet.create({
  container: {
    flexGrow: 1,
    gap: 18,
    paddingBottom: 118,
    paddingHorizontal: 18,
    paddingTop: TOP_SAFE_PADDING,
  },
});
