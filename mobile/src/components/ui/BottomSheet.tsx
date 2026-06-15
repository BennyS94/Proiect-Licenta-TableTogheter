import { useEffect, useMemo, useRef } from "react";
import type { ReactNode } from "react";
import {
  Animated,
  Modal,
  PanResponder,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  useWindowDimensions,
  View,
} from "react-native";

import { colors } from "../../theme/colors";

type BottomSheetProps = {
  children: ReactNode;
  onClose: () => void;
  subtitle?: string;
  title?: string;
  titleAccessory?: ReactNode;
  visible: boolean;
};

export function BottomSheet({
  children,
  onClose,
  subtitle,
  title,
  titleAccessory,
  visible,
}: BottomSheetProps) {
  const { height } = useWindowDimensions();
  const maxHeight = Math.round(height * 0.76);
  const translateY = useRef(new Animated.Value(0)).current;
  const panResponder = useMemo(
    () =>
      PanResponder.create({
        onStartShouldSetPanResponder: () => true,
        onStartShouldSetPanResponderCapture: () => true,
        onMoveShouldSetPanResponderCapture: (_event, gesture) =>
          gesture.dy > 8 && Math.abs(gesture.dy) > Math.abs(gesture.dx),
        onMoveShouldSetPanResponder: (_event, gesture) =>
          gesture.dy > 8 && Math.abs(gesture.dy) > Math.abs(gesture.dx),
        onPanResponderGrant: () => {
          translateY.stopAnimation();
        },
        onPanResponderMove: (_event, gesture) => {
          translateY.setValue(Math.max(0, gesture.dy));
        },
        onPanResponderRelease: (_event, gesture) => {
          if (gesture.dy > 90 || gesture.vy > 0.75) {
            Animated.timing(translateY, {
              duration: 180,
              toValue: maxHeight,
              useNativeDriver: true,
            }).start(() => {
              translateY.setValue(0);
              onClose();
            });
            return;
          }
          Animated.spring(translateY, {
            bounciness: 4,
            speed: 16,
            toValue: 0,
            useNativeDriver: true,
          }).start();
        },
        onPanResponderTerminate: () => {
          Animated.spring(translateY, {
            bounciness: 4,
            speed: 16,
            toValue: 0,
            useNativeDriver: true,
          }).start();
        },
      }),
    [maxHeight, onClose, translateY],
  );

  useEffect(() => {
    if (visible) {
      translateY.setValue(0);
    }
  }, [translateY, visible]);

  return (
    <Modal
      animationType="slide"
      onRequestClose={onClose}
      statusBarTranslucent
      transparent
      visible={visible}
    >
      <View style={styles.root}>
        <Pressable
          accessibilityLabel="Dismiss sheet"
          accessibilityRole="button"
          onPress={onClose}
          style={styles.backdrop}
        />
        <Animated.View
          accessibilityViewIsModal
          style={[styles.sheet, { maxHeight, transform: [{ translateY }] }]}
        >
          <View style={styles.gestureZone} {...panResponder.panHandlers}>
            <View style={styles.handle} />
            {title ? (
              <View style={styles.titleRow}>
                <View style={styles.titleBlock}>
                  <Text numberOfLines={2} style={styles.title}>
                    {title}
                  </Text>
                  {subtitle ? (
                    <Text numberOfLines={1} style={styles.subtitle}>
                      {subtitle}
                    </Text>
                  ) : null}
                </View>
                {titleAccessory ? <View style={styles.titleAccessory}>{titleAccessory}</View> : null}
              </View>
            ) : (
              <View style={styles.titleSpacer} />
            )}
          </View>
          <ScrollView
            bounces={false}
            contentContainerStyle={styles.content}
            showsVerticalScrollIndicator={false}
          >
            {children}
          </ScrollView>
        </Animated.View>
      </View>
    </Modal>
  );
}

const styles = StyleSheet.create({
  backdrop: {
    backgroundColor: "rgba(17, 24, 39, 0.38)",
    bottom: 0,
    left: 0,
    position: "absolute",
    right: 0,
    top: 0,
  },
  content: {
    gap: 14,
    paddingBottom: 22,
  },
  gestureZone: {
    marginBottom: 0,
    marginHorizontal: -20,
    minHeight: 72,
    paddingBottom: 6,
    paddingHorizontal: 20,
  },
  handle: {
    alignSelf: "center",
    backgroundColor: "#C7D8BE",
    borderRadius: 999,
    height: 5,
    marginBottom: 12,
    width: 44,
  },
  root: {
    flex: 1,
    justifyContent: "flex-end",
  },
  sheet: {
    backgroundColor: colors.background,
    borderTopLeftRadius: 24,
    borderTopRightRadius: 24,
    elevation: 16,
    paddingHorizontal: 20,
    paddingTop: 12,
    shadowColor: "#111827",
    shadowOffset: { width: 0, height: -8 },
    shadowOpacity: 0.18,
    shadowRadius: 18,
  },
  subtitle: {
    color: colors.mutedSoft,
    fontSize: 13,
    fontWeight: "800",
    marginTop: 4,
  },
  titleAccessory: {
    alignItems: "flex-end",
    flexShrink: 0,
    paddingTop: 4,
  },
  titleBlock: {
    flex: 1,
    minWidth: 0,
  },
  titleRow: {
    alignItems: "flex-start",
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  title: {
    color: colors.text,
    flexShrink: 1,
    fontSize: 24,
    fontWeight: "900",
    lineHeight: 29,
  },
  titleSpacer: {
    flex: 1,
  },
});
