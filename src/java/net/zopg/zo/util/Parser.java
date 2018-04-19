package net.zopg.zo.util;

import clojure.lang.ExceptionInfo;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.ArrayList;

// based on parse_ident and others in backend/utils/adt/misc.c

public class Parser {

    public static final String ARRAY_SUFFIX = "[]";

    public static IPersistentMap parseIdent(String nameString) throws ExceptionInfo {
      if (nameString.length() == 0) {
          throw new ExceptionInfo("string is not a valid identifier", makeExData(nameString));
      }

      if (nameString.indexOf('\0') != -1) {
          throw new ExceptionInfo("string is not a valid identifier (must not contain null byte)",
                                  makeExData(nameString));
      }

      boolean isArray = nameString.endsWith(ARRAY_SUFFIX);
      String identifier = isArray ? nameString.substring(0, nameString.length() - 2) : nameString;

      if (identifier.length() == 0) {
          throw new ExceptionInfo("string is not a valid identifier: \"{0}\"", makeExData(nameString));
      }

      ArrayList<String> nameParts = new ArrayList<>();

      boolean missingIdent = true;
      String remainder = identifier;
      boolean isQuoted = false;

      while (true) {
        if (remainder.charAt(0) == '"') {
          isQuoted = true;
          int quoteIdx = 0;
          String current = remainder.substring(1);
          while (true) {
            quoteIdx = remainder.indexOf('"', quoteIdx + 1);
            if (quoteIdx == -1) {
              // String has unclosed quotes.
                throw new ExceptionInfo("invalid type name: \"{0}\"", makeExData(nameString));
            }
            if (remainder.length() <= quoteIdx + 1) {
              break;
            }
            if (remainder.charAt(quoteIdx + 1) != '"') {
              break;
            }
            quoteIdx++;
          }
          remainder = remainder.substring(quoteIdx + 1);
          String currentName = current.substring(0, quoteIdx - 1).replaceAll("\"\"", "\"");
          if (currentName.length() == 0) {
              throw new ExceptionInfo("invalid type name (quoted identifier must not be empty): \"{0}\"", makeExData(nameString));
          }
          nameParts.add(currentName);
          missingIdent = false;
        } else if (isIdentStart(remainder.charAt(0))) {
          int charIdx = 1;
          int length = remainder.length();
          while (charIdx < length && isIdentContinuation(remainder.charAt(charIdx))) {
            charIdx++;
          }
          String currentName = remainder.substring(0, charIdx);
          nameParts.add(currentName);
          remainder = remainder.substring(charIdx);
          missingIdent = false;
        }

        if (missingIdent) {
          if (remainder.charAt(0) == '.') {
              throw new ExceptionInfo("invalid type name (no identifier before \".\"): \"{0}\"", makeExData(nameString));
          } else {
              throw new ExceptionInfo("invalid type name", makeExData(nameString));
          }
        }

        if (remainder.length() == 0) {
          break;
        } else if (remainder.charAt(0) == '.') {
          remainder = remainder.substring(1);
          if (remainder.length() == 0) {
              throw new ExceptionInfo("invalid type name (no identifier after \".\")",
                                      makeExData(nameString));
          }
        } else {
            throw new ExceptionInfo("invalid type name", makeExData(nameString));
        }
      }

      int namePartCount = nameParts.size();

      Keyword namespaceKey = Keyword.intern("nspname");
      Keyword typeNameKey = Keyword.intern("typname");
      Keyword isArrayKey = Keyword.intern("array?");
      Keyword isSimpleKey = Keyword.intern("simple?");
      
      if (namePartCount == 1) {
          return new PersistentArrayMap(new Object[]{namespaceKey, null,
                                                     typeNameKey, nameParts.get(0),
                                                     isArrayKey, isArray,
                                                     isSimpleKey, !isQuoted});
      }

      if (namePartCount == 2) {
          return new PersistentArrayMap(new Object[]{namespaceKey, nameParts.get(0),
                                                     typeNameKey, nameParts.get(1),
                                                     isArrayKey, isArray,
                                                     isSimpleKey, false});
      }

      throw new ExceptionInfo("invalid type name: \"{0}\"", makeExData(nameString));
    }

    private static IPersistentMap makeExData(String nameString) {
        Keyword stringKey = Keyword.intern("string");
        IPersistentMap exData = new PersistentArrayMap(new Object[]{stringKey, nameString});
        return exData;
    }

    private static boolean isIdentStart(char c) {
      return (c != '.' && c != '"');
    }

    private static boolean isIdentContinuation(char c) {
      return (c != '.');
    }

}
