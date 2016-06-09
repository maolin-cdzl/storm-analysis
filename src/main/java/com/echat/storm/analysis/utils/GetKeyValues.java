package com.echat.storm.analysis.utils;

import java.util.Map;
import java.util.HashMap;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.PatternMatcherInput;


public class GetKeyValues {
	private Pattern keyValuePattern;

	public GetKeyValues() {
		try {
			PatternCompiler compiler = new Perl5Compiler();
			keyValuePattern = compiler.compile("(\\w+)=\\(([^\\)]*)\\)");
		} catch( MalformedPatternException e ) {
			//log.error(e.getMessage());
		}
	}

	public Map<String,String> getKeys(String content) {
		Map<String,String> keys = new HashMap<String,String>();
		PatternMatcher pm = new Perl5Matcher();
		PatternMatcherInput matcherInput = new PatternMatcherInput(content);  

		while( pm.contains(matcherInput,keyValuePattern) ) {
			MatchResult mr = pm.getMatch();  
			keys.put(mr.group(1),mr.group(2));
		}
		return keys;
	}
}


