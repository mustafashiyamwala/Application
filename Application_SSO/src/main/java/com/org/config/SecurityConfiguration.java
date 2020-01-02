package com.org.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.authentication.configuration.EnableGlobalAuthentication;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.session.HttpSessionEventPublisher;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import com.org.security.AuthenticationFilters;
import com.org.security.CustomIPAuthenticationProvider;
import com.org.security.LoginFailureHandler;
import com.org.security.LoginSuccessHandler;
import com.org.security.LogoutSuccessHandlers;
import com.org.security.UserDetailsAuthenticationProvider;
import com.org.service.LoginService;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Configuration
@EnableWebSecurity(debug = false)
@EnableGlobalMethodSecurity(prePostEnabled = true)
@ComponentScan(basePackages = { "com.org.security" })
public class SecurityConfiguration extends WebSecurityConfigurerAdapter {

	@Autowired
	private LoginService loginService;

	@Autowired
	private CustomIPAuthenticationProvider customIPAuthenticationProvider;

	@Bean()
	@Override
	public AuthenticationManager authenticationManagerBean() throws Exception {
		return super.authenticationManagerBean();
	}

	@Override
	protected void configure(AuthenticationManagerBuilder authenticationManagerBuilder) throws Exception {

		// PasswordEncoder encoder =
		// PasswordEncoderFactories.createDelegatingPasswordEncoder();
		/*
		 * authenticationManagerBuilder.inMemoryAuthentication().withUser("user").
		 * password(encoder().encode("user1Pass"))
		 * .roles("USER").and().withUser("admin").password(encoder().encode("admin1Pass"
		 * )).roles("ADMIN");
		 */

		// authenticationManagerBuilder.authenticationProvider(customIPAuthenticationProvider);
		authenticationManagerBuilder
				.authenticationProvider(new UserDetailsAuthenticationProvider(encoder(), loginService));
	}

	@Bean
	public PasswordEncoder encoder() {
		return new BCryptPasswordEncoder();
	}

	@Bean
	public AuthenticationFilters authenticationFilters() throws Exception {
		AuthenticationFilters authenticationFilters = new AuthenticationFilters();
		authenticationFilters.setAuthenticationManager(authenticationManagerBean());

		log.info("Handles Login Request");
		authenticationFilters
				.setRequiresAuthenticationRequestMatcher(new AntPathRequestMatcher("/validateUser/access"));
		authenticationFilters.setUsernameParameter("username");
		authenticationFilters.setPasswordParameter("password");
		authenticationFilters.setAuthenticationSuccessHandler(new LoginSuccessHandler());
		authenticationFilters.setAuthenticationFailureHandler(new LoginFailureHandler());

		return authenticationFilters;
	}

	@Override
	public void configure(WebSecurity webSecurity) {
		webSecurity.ignoring().antMatchers("/templates/*");
	}

	@Override
	protected void configure(HttpSecurity httpSecurity) throws Exception {
		httpSecurity.csrf().disable().authorizeRequests()

				.antMatchers("/registration/**")
				.access("isAuthenticated() and hasAuthority('READ_PRIVILEGE') and hasAuthority('WRITE_PRIVILEGE')")
				
				.antMatchers("/request/authenticate**", "/request/validate/session**").permitAll()

				.anyRequest().authenticated()

				// .antMatchers("/service/**").hasIpAddress("10.10.10.10")
				// .access("isAuthenticated() and hasIpAddress('11.11.11.11')")

				.and().addFilterBefore(authenticationFilters(), UsernamePasswordAuthenticationFilter.class)

				.formLogin().loginPage("/request/authenticate?page=login")
				// .loginProcessingUrl("/login")
				// .usernameParameter("username").passwordParameter("password")
				// .defaultSuccessUrl("/request/user/home", true)
				// .successHandler(new LoginSuccessHandler())
				// .failureUrl("/request/user/val/logout?error=true")
				// .failureHandler(new CustomAuthenticationFailure())

				.and().logout().logoutUrl("/request/authenticate?page=logout")

				.invalidateHttpSession(false).deleteCookies("JSESSIONID")
				// .logoutSuccessUrl("/request/user/val/logout?error=true");
				.logoutSuccessHandler(new LogoutSuccessHandlers())

				.and().rememberMe().key("uniqueAndSecret").rememberMeParameter("remember").tokenValiditySeconds(86400)

				.and().sessionManagement().sessionFixation().none()
				.sessionCreationPolicy(SessionCreationPolicy.IF_REQUIRED)
				.invalidSessionUrl("/request/validate/session?session=invalid").maximumSessions(1)
				.expiredUrl("/request/validate/session?session=expired");
	}

	/*
	 * @Bean public HttpSessionEventPublisher httpSessionEventPublisher() { return
	 * new HttpSessionEventPublisher(); }
	 */

	/*
	 * @Override protected void configure(AuthenticationManagerBuilder
	 * authenticationManagerBuilder) throws Exception { DaoAuthenticationProvider
	 * daoAuthenticationProvider = new DaoAuthenticationProvider();
	 * daoAuthenticationProvider.setUserDetailsService(loginService);
	 * daoAuthenticationProvider.setPasswordEncoder(encoder());
	 * authenticationManagerBuilder.authenticationProvider(daoAuthenticationProvider
	 * );
	 * 
	 * }
	 */
}
