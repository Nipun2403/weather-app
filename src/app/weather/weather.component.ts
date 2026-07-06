import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatTableModule } from '@angular/material/table';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatChipsModule } from '@angular/material/chips';
import { WeatherService, WeatherForecast } from '../services/weather.service';

@Component({
  selector: 'app-weather',
  standalone: true,
  imports: [
    CommonModule,
    MatTableModule,
    MatCardModule,
    MatButtonModule,
    MatProgressSpinnerModule,
    MatChipsModule,
  ],
  templateUrl: './weather.component.html',
  styleUrl: './weather.component.css',
})
export class WeatherComponent implements OnInit {
  displayedColumns: string[] = ['date', 'temperatureC', 'temperatureF', 'summary'];
  forecasts: WeatherForecast[] = [];
  loading = true;
  error: string | null = null;

  constructor(
    private weatherService: WeatherService,
    private cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.loadForecasts();
  }

  loadForecasts(): void {
    this.loading = true;
    this.error = null;
    this.weatherService.getForecasts().subscribe({
      next: (data) => {
        this.forecasts = data;
        this.loading = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.error = 'Failed to load weather data. Please try again later.';
        this.loading = false;
        this.cdr.detectChanges();
      },
    });
  }

  isHot(tempC: number): boolean {
    return tempC > 30;
  }
}
