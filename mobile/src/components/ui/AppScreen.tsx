import type { ReactNode } from "react";
import type { StyleProp, ViewStyle } from "react-native";
import { Platform, ScrollView, StatusBar, StyleSheet, View } from "react-native";

import { FLOATING_NAV_SCREEN_BOTTOM_PADDING } from "../navigation/FloatingNav";
import { colors } from "../../theme/colors";

type AppScreenProps = {
  children: ReactNode;
  contentContainerStyle?: StyleProp<ViewStyle>;
};

export function AppScreen({ children, contentContainerStyle }: AppScreenProps) {
  return (
    <View style={styles.shell}>
      <ScrollView
        alwaysBounceVertical={false}
        bounces={false}
        contentContainerStyle={[styles.container, contentContainerStyle]}
        fadingEdgeLength={TOP_FADE_HEIGHT}
        overScrollMode="never"
        style={styles.scroll}
      >
        {children}
      </ScrollView>
      <View pointerEvents="none" style={styles.topFade}>
        <View style={styles.topFadeSolid} />
        <View style={styles.topFadeStep1} />
        <View style={styles.topFadeStep2} />
        <View style={styles.topFadeStep3} />
        <View style={styles.topFadeStep4} />
        <View style={styles.topFadeStep5} />
      </View>
    </View>
  );
}

const TOP_SAFE_PADDING =
  Platform.OS === "android" ? 30 : Math.max(StatusBar.currentHeight ?? 0, 24) + 8;
const TOP_FADE_BLEND_HEIGHT = 6;
const TOP_FADE_SOLID_HEIGHT = 4;
const TOP_FADE_HEIGHT = TOP_FADE_SOLID_HEIGHT + TOP_FADE_BLEND_HEIGHT;

const styles = StyleSheet.create({
  shell: {
    backgroundColor: colors.background,
    flex: 1,
    paddingTop: TOP_SAFE_PADDING,
  },
  topFade: {
    height: TOP_FADE_HEIGHT,
    left: 0,
    position: "absolute",
    right: 0,
    top: 0,
  },
  topFadeSolid: {
    backgroundColor: colors.background,
    height: TOP_FADE_SOLID_HEIGHT,
  },
  topFadeStep1: {
    backgroundColor: "rgba(250, 252, 247, 0.86)",
    flex: 1,
  },
  topFadeStep2: {
    backgroundColor: "rgba(250, 252, 247, 0.68)",
    flex: 1,
  },
  topFadeStep3: {
    backgroundColor: "rgba(250, 252, 247, 0.48)",
    flex: 1,
  },
  topFadeStep4: {
    backgroundColor: "rgba(250, 252, 247, 0.28)",
    flex: 1,
  },
  topFadeStep5: {
    backgroundColor: "rgba(250, 252, 247, 0.12)",
    flex: 1,
  },
  scroll: {
    backgroundColor: colors.background,
    flex: 1,
  },
  container: {
    backgroundColor: colors.background,
    flexGrow: 1,
    gap: 18,
    paddingBottom: FLOATING_NAV_SCREEN_BOTTOM_PADDING,
    paddingHorizontal: 18,
  },
});
